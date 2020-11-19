use std::fs;
use std::path::PathBuf;
use std::io::{Write, BufWriter};

use itertools::Itertools;

const MESSAGE_CAPNP_SRC: &str = "src/bft/communication/serialize/capnp/message.capnp";
const ERROR_KIND_DST: &str = "error_kind.rs";

fn main() {
    generate_error_kinds();
    generate_message_from_capnp();
}

fn generate_message_from_capnp() {
    // check if we need to compile capnp
    match std::env::var("CARGO_FEATURE_SERIALIZE_CAPNP") {
        Ok(_) => (),
        _ => return,
    };

    // recompile capnp message into rust when the source changes
    println!("cargo:rerun-if-changed={}", MESSAGE_CAPNP_SRC);
    capnpc::CompilerCommand::new()
        .file(MESSAGE_CAPNP_SRC)
        .run()
        .unwrap();
}

fn generate_error_kinds() {
    fn generate(mut pbuf: &mut PathBuf, mut name_buf: &mut Vec<String>, mut buf: &mut BufWriter<fs::File>) {
        let dir_ents = fs::read_dir(&mut pbuf).unwrap();

        for ent in dir_ents {
            let dir_name = ent
                .iter()
                .flat_map(|ent| ent.file_type().map(|t| (t, ent.file_name())))
                .map(|(t, name)| if t.is_dir() { Some(name) } else { None })
                .nth(0)
                .unwrap_or(None);
            let orig_dir_name = match dir_name {
                Some(n) => n,
                _ => continue,
            };
            let dir_name = orig_dir_name
                .as_os_str()
                .to_str()
                .unwrap()
                .split('_')
                .flat_map(|part| {
                    let (first, rest) = part.split_at(1);
                    first
                        .chars()
                        .map(|ch| ch.to_ascii_uppercase())
                        .chain(rest.chars())
                })
                .join("");
            name_buf.push(dir_name);
            writeln!(buf, "    {},", name_buf.join("")).unwrap();
            pbuf.push(orig_dir_name);
            generate(&mut pbuf, &mut name_buf, &mut buf);
        }

        pbuf.pop();
        name_buf.pop();
    }

    let path_buf = {
        let mut b = PathBuf::from(std::env::var("OUT_DIR").unwrap());
        b.push(ERROR_KIND_DST);
        b
    };
    let file = fs::File::create(path_buf).unwrap();
    let mut buf = BufWriter::new(file);

    let mut path_buf = {
        let mut b = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
        b.push("src");
        b.push("bft");
        b
    };
    let mut name_buf = Vec::new();

    writeln!(&mut buf, r#"/// Includes a list of all the library software components.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ErrorKind {{"#).unwrap();
    generate(&mut path_buf, &mut name_buf, &mut buf);
    writeln!(&mut buf, "}}").unwrap();
}
