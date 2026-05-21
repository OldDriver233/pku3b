use std::sync::Arc;

use anyhow::Context;

use super::*;

#[derive(clap::Args)]
pub struct CommandDocument {
    /// 强制刷新
    #[arg(short, long, default_value = "false")]
    force: bool,

    #[command(subcommand)]
    command: DocumentCommands,

    /// 手机令牌码。当需要使用 OTP 登录，但未提供此参数时，将会从命令行交互式读取 OTP 码。
    #[arg(long, default_value = "")]
    otp_code: String,
}

#[derive(Subcommand)]
enum DocumentCommands {
    #[command(visible_alias("ls"))]
    List {
        #[arg(long, default_value = "false")]
        all_term: bool,
    },
    #[command(visible_alias("down"))]
    Download {
        #[arg(group = "download-type")]
        id: Option<String>,
        /// 文件下载目录 (支持相对路径)
        #[arg(short, long, default_value = ".")]
        dir: std::path::PathBuf,
        #[arg(long, default_value = "false")]
        all_term: bool,
    }
}

async fn get_contents(
    c: &Course,
    pb: indicatif::ProgressBar,
) -> anyhow::Result<Vec<CourseContent>> {
    let fut = async {
        let mut s = c.content_stream();

        // let pb = pbar::new(s.len() as u64).with_message("search contents");
        pb.set_length(s.len() as u64);
        pb.tick();

        let mut contents = Vec::new();
        while let Some(batch) = s.next_batch().await {
            contents.extend(batch);

            pb.set_length(s.len() as u64);
            pb.set_position(s.num_finished() as u64);
            pb.tick();
        }

        pb.finish_with_message("done.");
        Ok(contents)
    };

    let data = utils::with_cache(
        &format!("get_course_contents_{}", c.meta().id()),
        c.client().cache_ttl(),
        fut,
    )
    .await?;

    Ok(data.into_iter().map(|data| c.build_content(data)).collect())
}

async fn get_documents(c: &Course, pb: indicatif::ProgressBar) -> anyhow::Result<Vec<CourseDocumentHandle>> {
    let r = get_contents(c, pb)
        .await?
        .into_iter()
        .filter_map(|c| c.into_document_opt())
        .collect();
    Ok(r)
}

async fn get_courses_and_documents(
    force: bool,
    cur_term: bool,
    otp_code: String,
) -> anyhow::Result<Vec<(Course, Vec<(String, CourseDocument)>)>> {
    let courses = load_courses(force, cur_term, otp_code).await?;

    // fetch each course concurrently
    let m = indicatif::MultiProgress::new();
    let pb = m.add(pbar::new(courses.len() as u64)).with_prefix("All");
    let futs = courses.into_iter().map(async |c| -> anyhow::Result<_> {
        let c = c.get().await.context("fetch course")?;
        let documents = get_documents(
            &c, 
            m.add(pbar::new(0).with_prefix(c.meta().name().to_owned())),
        )
        .await
        .with_context(|| format!("fetch document handles of {}", c.meta().title()))?;

        pb.inc_length(documents.len() as u64);
        let futs = documents.into_iter().map(async |d| -> anyhow::Result<_> {
            let id = d.id();
            let r = d.get().await.context("fetch document")?;
            pb.inc(1);
            Ok((id, r))
        });
        let documents = try_join_all(futs).await?;

        pb.inc(1);
        Ok((c, documents))
    });

    let courses = try_join_all(futs).await?;
    pb.finish_and_clear();
    m.clear().unwrap();
    drop(pb);
    drop(m);

    Ok(courses)
}

pub async fn run(cmd: CommandDocument) -> anyhow::Result<()> {
    match cmd.command {
        DocumentCommands::List { all_term } => list(cmd.force, !all_term, cmd.otp_code).await?,
        DocumentCommands::Download { id, dir, all_term } => {
            download(id.as_deref(), &dir, cmd.force, all_term, !all_term, cmd.otp_code).await?
        }
    }
    Ok(())
}

pub async fn list(force: bool, cur_term: bool, otp_code: String) -> anyhow::Result<()> {
    let courses = get_courses_and_documents(force, cur_term, otp_code).await?;
    let all_documents = courses
        .iter()
        .flat_map(|(c, documents)| {
            documents
                .iter()
                .map(move |(id, d)| (c.to_owned(), id.to_owned(), d.clone()))
        })
        .collect::<Vec<_>>();
    let mut outbuf = Vec::new();
    let title = "所有课程文档";
    let total = all_documents.len();
    writeln!(outbuf, "{D}>{D:#} {B}{title} ({total}){B:#} {D}<{D:#}\n")?;

    for (c, id, d) in all_documents.iter() {
        write_course_document(&mut outbuf, id, c, d)?;
    }

    buf_try!(@try fs::stdout().write_all(outbuf).await);
    Ok(())
}

type DocumentItem = (Arc<Course>, String, CourseDocument);

async fn fetch_documents(
    force: bool,
    all: bool,
    cur_term: bool,
    otp_code: String,
) -> anyhow::Result<Vec<DocumentItem>> {
    let courses = get_courses_and_documents(force, cur_term, otp_code).await?;
    let all_documents = courses
        .into_iter()
        .flat_map(|(c, documents)| {
            let c = Arc::new(c);
            documents
                .into_iter()
                .map(move |(id, d)| (c.clone(), id, d))
        })
        .collect::<Vec<_>>();

    Ok(all_documents)
}

async fn select_document(
    mut items: Vec<DocumentItem>
) -> anyhow::Result<DocumentItem> {
    if items.is_empty() {
        anyhow::bail!("document not found");
    }

    let mut options = Vec::new();
    for (idx, (c, id, d)) in items.iter().enumerate() {
        let mut outbuf = Vec::new();
        write!(outbuf, "[{}] ", idx + 1)?;
        write_document_header_ln(&mut outbuf, id, c, d).context("io error")?;
        options.push(String::from_utf8(outbuf).unwrap());
    }

    let s = inquire::Select::new("请选择要下载的文档", options).raw_prompt()?;
    let idx = s.index;
    let r = items.swap_remove(idx);

    Ok(r)
}

pub async fn download(
    id: Option<&str>,
    dir: &std::path::Path,
    force: bool,
    all: bool,
    cur_term: bool,
    otp_code: String,
) -> anyhow::Result<()> {
    let items = fetch_documents(force, all, cur_term, otp_code).await?;
    let a = match id {
        Some(id) => match items.into_iter().find(|x| x.1 == id) {
            Some(r) => r,
            None => anyhow::bail!("document with id {} not found", id),
        },
        None => select_document(items).await?,
    };

    let sp = pbar::new_spinner();
    download_data(sp, dir, &a.2).await?;
    Ok(())
}

async fn download_data(
    sp: pbar::AsyncSpinner,
    dir: &std::path::Path,
    d: &CourseDocument,
) -> anyhow::Result<()> {
    if !dir.exists() {
        compio::fs::create_dir_all(dir).await?;
    }
    let atts = d.attachments();
    let tot = atts.len();
    for (id, (name, uri)) in atts.iter().enumerate() {
        sp.set_message(format!(
            "[{}/{tot}] downloading attachment '{name}'...",
            id + 1
        ));
        d.download_attachment(uri, &dir.join(name))
            .await
            .with_context(|| format!("download attachment '{name}'"))?;
    }

    drop(sp);
    println!("Done.");
    Ok(())
}

fn write_document_header_ln(
    buf: &mut Vec<u8>,
    id: &str,
    c: &Course,
    d: &CourseDocument
) -> std::io::Result<()> {
    write!(buf, "{BL}{B}{}{B:#}{BL:#} {D}>{D:#} ", c.meta().name())?;
    write!(buf, "{BL}{B}{}{B:#}{BL:#}", d.title())?;
    writeln!(buf, " {D}{id}{D:#}")?;
    Ok(())
}

fn write_course_document(
    buf: &mut Vec<u8>,
    id: &str,
    c: &Course,
    d: &CourseDocument
) -> std::io::Result<()> {
    write_document_header_ln(buf, id, c, d)?;
    if !d.descriptions().is_empty() {
        writeln!(buf)?;
        for p in d.descriptions() {
            writeln!(buf, "{p}")?;
        }
    }
    if !d.attachments().is_empty() {
        writeln!(buf)?;
        for (name, _) in d.attachments() {
            writeln!(buf, "{D}[附件]{D:#} {UL}{name}{UL:#}")?;
        }
    }
    writeln!(buf)?;
    Ok(())
}