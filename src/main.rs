//! Binary shim for the `noetl` / `ntl` commands.
//!
//! The command tree, the runtime ladder, and the local execution engine all
//! live in `src/lib.rs` so embedders (the PyO3 wheel on PyPI) run the same
//! code path this binary does.  See [`noetl::cli_main`].

fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(noetl::cli_main(std::env::args_os()) as u8)
}
