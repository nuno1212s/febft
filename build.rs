use std::fs;
use std::path::PathBuf;
use std::io::{Write, BufWriter};

use itertools::Itertools;

const ERROR_KIND_DST: &str = "error_kind.rs";

const MESSAGE_CAPNP_SRC: &str = "src/schemas/messages.capnp";
const PERSISTENT_CAPNP_SRC: &str = "src/schemas/objects.capnp";


fn main() {
    generate_error_kinds();
    
    // recompile capnp message into rust when the source changes
    println!("cargo:rerun-if-changed={}", MESSAGE_CAPNP_SRC);

    capnpc::CompilerCommand::new()
        .src_prefix("src/schemas")
        .file(MESSAGE_CAPNP_SRC)
        .file(PERSISTENT_CAPNP_SRC)
        .run()
        .unwrap();
}

fn generate_error_kinds() {
    // this function recursively outputs directory names
    // under src/bft/ into the ERROR_KIND_DST file,
    // erasing underscores, and setting title case for
    // words between them
    fn generate(mut pbuf: &mut PathBuf, mut name_buf: &mut Vec<String>, mut buf: &mut BufWriter<fs::File>) {
        let dir_ents = fs::read_dir(&mut pbuf).unwrap();

        for ent in dir_ents {
            let dir_name = ent
                .iter()
                .flat_map(|ent| ent.file_type().map(|t| (t, ent.file_name())))
                .map(|(t, name)| if t.is_dir() { Some(name) } else { None })
                .nth(0)
                .unwrap_or(None);
            let dir_name = match dir_name {
                Some(n) => n,
                _ => continue,
            };
            let kind_name = dir_name
                .to_str()
                .unwrap()
                .split('_')
                .flat_map(|part| {
                    // this ugly code sets a word contained
                    // in a string into title case
                    let (first, rest) = part.split_at(1);
                    first
                        .chars()
                        .map(|ch| ch.to_ascii_uppercase())
                        .chain(rest.chars())
                })
                .join("");

            // update buffers with the newly fetched dir name
            name_buf.push(kind_name);
            pbuf.push(dir_name);

            // output the generated kind name
            writeln!(buf, "    {},", name_buf.join("")).unwrap();

            // recursively generate names for other directories
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

    writeln!(&mut buf, r#"/// Includes a list of all the errors reported by this
/// crate's modules. Generated automatically with `build.rs`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ErrorKind {{"#).unwrap();
    generate(&mut path_buf, &mut name_buf, &mut buf);
    writeln!(&mut buf, "}}").unwrap();
}
