fn main() {
    // SQLx embeds migration bytes; additions and edits must invalidate cached builds.
    println!("cargo:rerun-if-changed=../../migrations");
}
