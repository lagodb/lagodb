use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Command;

use crate::{
    CONNECTORS_NAME, CONNECTORS_PACKAGE, DELTA_AM_NAME, DELTA_AM_PACKAGE,
    EXTENSION_NAME, EXTENSION_PACKAGE, RUNTIME_NAME, cargo_pgrx_info,
    install_runtime, pg_config_value, pg_major, prepend_path_env, run_command,
    usage_error,
};

#[derive(Clone, Copy)]
pub(crate) enum RegressionSuite {
    Iceberg,
    Connectors,
}

impl RegressionSuite {
    fn package(self) -> &'static str {
        match self {
            Self::Iceberg => EXTENSION_PACKAGE,
            Self::Connectors => CONNECTORS_PACKAGE,
        }
    }

    fn provider_libraries(self) -> String {
        match self {
            Self::Iceberg => format!("{EXTENSION_NAME},{DELTA_AM_NAME}"),
            Self::Connectors => CONNECTORS_NAME.to_owned(),
        }
    }
}

pub(crate) struct RegressionRunner {
    pg_version: OsString,
    pg_config: PathBuf,
    bindir: PathBuf,
}

impl RegressionRunner {
    pub(crate) fn prepare(pg_version: &OsStr) -> Result<Self, String> {
        pg_major(pg_version)?;
        install_runtime(pg_version)?;

        let pg_config = cargo_pgrx_info(pg_version, "pg-config")?;
        let bindir = pg_config_value(&pg_config, "--bindir")?;

        Ok(Self {
            pg_version: pg_version.to_owned(),
            pg_config,
            bindir,
        })
    }

    pub(crate) fn run(
        &self,
        suite: RegressionSuite,
        tests: &[OsString],
    ) -> Result<(), String> {
        if matches!(suite, RegressionSuite::Iceberg) {
            run_command(
                Command::new("cargo")
                    .arg("pgrx")
                    .arg("install")
                    .arg("--package")
                    .arg(DELTA_AM_PACKAGE)
                    .arg("--features")
                    .arg("pg_test")
                    .arg("--pg-config")
                    .arg(&self.pg_config),
            )?;
        }

        let mut command = Command::new("cargo");
        command
            .arg("pgrx")
            .arg("regress")
            .arg(&self.pg_version)
            .arg("--package")
            .arg(suite.package())
            .arg("--resetdb")
            .arg("--postgresql-conf")
            .arg(format!("shared_preload_libraries='{RUNTIME_NAME}'"))
            .arg("--postgresql-conf")
            .arg(format!(
                "pg_lakebase.provider_libraries='{}'",
                suite.provider_libraries()
            ));
        command.args(tests);
        prepend_path_env(&mut command, &self.bindir)?;

        run_command(&mut command)
    }
}

pub(crate) enum RegressionTarget {
    All,
    Suite {
        suite: RegressionSuite,
        tests: Vec<OsString>,
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
                    return Err(usage_error(&format!(
                        "test '{}' requires an explicit regression suite",
                        test.to_string_lossy()
                    )));
                }
                return Ok(Self::All);
            }
            Some("iceberg") => RegressionSuite::Iceberg,
            Some("connectors") => RegressionSuite::Connectors,
            _ => {
                return Err(usage_error(&format!(
                    "unknown regression suite '{}'",
                    suite_name.to_string_lossy()
                )));
            }
        };

        let tests = args.collect();

        Ok(Self::Suite { suite, tests })
    }

    pub(crate) fn includes_iceberg(&self) -> bool {
        matches!(
            self,
            Self::All
                | Self::Suite {
                    suite: RegressionSuite::Iceberg,
                    ..
                }
        )
    }

    pub(crate) fn run(self, runner: &RegressionRunner) -> Result<(), String> {
        match self {
            Self::All => {
                println!("=== lagodb-iceberg SQL regression (PostgreSQL) ===\n");
                runner.run(RegressionSuite::Iceberg, &[])?;
                println!("\n=== LagoDB connectors SQL regression (PostgreSQL) ===\n");
                runner.run(RegressionSuite::Connectors, &[])
            }
            Self::Suite { suite, tests } => runner.run(suite, &tests),
        }
    }
}
