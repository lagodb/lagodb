use std::env;
use std::ffi::{OsStr, OsString};
use std::process::ExitCode;

mod extension;
mod isolation;
mod pg;
mod process;
mod regress;
mod test_all;

use pg::{PgInstallation, PgMajor};
use regress::RegressionTarget;

fn main() -> ExitCode {
    match Task::parse(env::args_os().skip(1)).and_then(Task::run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(1)
        }
    }
}

enum Task {
    TestAll(PgMajor),
    Regress {
        pg: PgMajor,
        target: RegressionTarget,
    },
    Isolation {
        pg: PgMajor,
        specs: Vec<OsString>,
    },
}

impl Task {
    fn parse(args: impl Iterator<Item = OsString>) -> Result<Self, String> {
        let mut args = args.peekable();
        match args.next().as_deref() {
            Some(command) if command == OsStr::new("isolation") => {
                let pg = PgMajor::from_args(&mut args);
                let specs = args.collect();
                Ok(Self::Isolation { pg, specs })
            }
            Some(command) if command == OsStr::new("test-all") => {
                let pg = PgMajor::from_args(&mut args);
                ensure_no_extra_args(args)?;
                Ok(Self::TestAll(pg))
            }
            Some(command) if command == OsStr::new("regress") => {
                let pg = PgMajor::from_args(&mut args);
                let target = RegressionTarget::parse(args)
                    .map_err(|message| usage_error(&message))?;
                Ok(Self::Regress { pg, target })
            }
            Some(command) => Err(usage_error(&format!(
                "unknown command '{}'",
                command.to_string_lossy()
            ))),
            None => Err(usage_error("missing command")),
        }
    }

    fn run(self) -> Result<(), String> {
        match self {
            Self::TestAll(pg) => test_all::run(&PgInstallation::resolve(pg)?),
            Self::Regress { pg, target } => {
                target.execute(&PgInstallation::resolve(pg)?)
            }
            Self::Isolation { pg, specs } => {
                isolation::run(&PgInstallation::resolve(pg)?, &specs)
            }
        }
    }
}

fn usage_error(message: &str) -> String {
    format!(
        "{message}\n\n\
         usage:\n  \
           cargo xtask test-all [pg-version]\n  \
           cargo xtask regress [pg-version] [all]\n  \
           cargo xtask regress [pg-version] <iceberg|connectors> [test]\n  \
           cargo xtask isolation [pg-version] [spec ...]\n\n\
         pg-version: pg15, pg16, pg17 or pg18 (default)\n\n\
         examples:\n  \
           cargo xtask test-all\n  \
           cargo xtask test-all pg16\n  \
           cargo xtask regress pg17\n  \
           cargo xtask regress pg17 iceberg worker\n  \
           cargo xtask regress pg17 connectors compression\n  \
           cargo xtask isolation pg17\n  \
           cargo xtask isolation pg17 cas_retry_stress"
    )
}

fn ensure_no_extra_args(
    mut args: impl Iterator<Item = OsString>,
) -> Result<(), String> {
    if let Some(arg) = args.next() {
        return Err(usage_error(&format!(
            "unexpected argument '{}'",
            arg.to_string_lossy()
        )));
    }
    Ok(())
}
