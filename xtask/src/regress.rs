use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

use crate::extension::Extension;
use crate::pg::PgInstallation;
use crate::process::{cargo_pgrx, prepend_path_env, run_command};

#[derive(Clone, Copy)]
pub(crate) enum RegressionSuite {
    Iceberg,
    Connectors,
}

impl RegressionSuite {
    const fn extension(self) -> Extension {
        match self {
            Self::Iceberg => Extension::Iceberg,
            Self::Connectors => Extension::Connectors,
        }
    }

    fn provider_libraries(self) -> String {
        match self {
            Self::Iceberg => format!(
                "{},{}",
                Extension::Iceberg.library(),
                Extension::DeltaAm.library()
            ),
            Self::Connectors => Extension::Connectors.library().to_owned(),
        }
    }
}

pub(crate) struct RegressionRunner<'a> {
    pg: &'a PgInstallation,
    bindir: PathBuf,
}

impl<'a> RegressionRunner<'a> {
    pub(crate) fn new(pg: &'a PgInstallation) -> Result<Self, String> {
        let bindir = pg.config_value("--bindir")?;
        Ok(Self { pg, bindir })
    }

    pub(crate) fn run(
        &self,
        suite: RegressionSuite,
        test: Option<&OsStr>,
    ) -> Result<(), String> {
        if matches!(suite, RegressionSuite::Iceberg) {
            self.pg.install_pg_test(Extension::DeltaAm)?;
        }

        let mut command = cargo_pgrx();
        command
            .arg("regress")
            .arg(self.pg.label())
            .arg("--package")
            .arg(suite.extension().package())
            .arg("--resetdb")
            .arg("--postgresql-conf")
            .arg(format!(
                "shared_preload_libraries='{}'",
                Extension::Runtime.library()
            ))
            .arg("--postgresql-conf")
            .arg(format!(
                "lagodb.provider_libraries='{}'",
                suite.provider_libraries()
            ));
        if let Some(test) = test {
            command.arg(test);
        }
        prepend_path_env(&mut command, &self.bindir)?;

        run_command(&mut command)
    }
}

pub(crate) enum RegressionTarget {
    All,
    Suite {
        suite: RegressionSuite,
        test: Option<OsString>,
    },
}

impl RegressionTarget {
    pub(crate) fn parse(
        mut args: impl Iterator<Item = OsString>,
    ) -> Result<Self, String> {
        let Some(suite_name) = args.next() else {
            return Ok(Self::All);
        };

        let suite = match suite_name.to_str() {
            Some("all") => {
                if let Some(test) = args.next() {
                    return Err(format!(
                        "test '{}' requires an explicit regression suite",
                        test.to_string_lossy()
                    ));
                }
                return Ok(Self::All);
            }
            Some("iceberg") => RegressionSuite::Iceberg,
            Some("connectors") => RegressionSuite::Connectors,
            _ => {
                return Err(format!(
                    "unknown regression suite '{}'",
                    suite_name.to_string_lossy()
                ));
            }
        };

        let test = args.next();
        if let Some(extra) = args.next() {
            return Err(format!(
                "cargo pgrx regress accepts one test name; unexpected '{}'",
                extra.to_string_lossy()
            ));
        }

        Ok(Self::Suite { suite, test })
    }

    fn includes_iceberg(&self) -> bool {
        matches!(
            self,
            Self::All
                | Self::Suite {
                    suite: RegressionSuite::Iceberg,
                    ..
                }
        )
    }

    pub(crate) fn execute(self, pg: &PgInstallation) -> Result<(), String> {
        if self.includes_iceberg() {
            pg.prepare_injection_points()?;
        }
        pg.install(Extension::Runtime)?;
        let runner = RegressionRunner::new(pg)?;

        match self {
            Self::All => {
                println!("=== lagodb-iceberg SQL regression (PostgreSQL) ===\n");
                runner.run(RegressionSuite::Iceberg, None)?;
                println!("\n=== LagoDB connectors SQL regression (PostgreSQL) ===\n");
                runner.run(RegressionSuite::Connectors, None)
            }
            Self::Suite { suite, test } => runner.run(suite, test.as_deref()),
        }
    }
}
