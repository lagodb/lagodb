use std::ffi::{OsStr, OsString};
use std::fs;
use std::iter::Peekable;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::extension::Extension;
use crate::process::{cargo_pgrx, run_command};

/// PostgreSQL major selected for one xtask invocation.
///
/// Selection follows cargo-pgrx: an optional leading `pgXX` argument, otherwise
/// the `#[default]` variant. The label is both the cargo-pgrx selector and the
/// Cargo feature. cargo-pgrx runs any other spelling as a test-name filter on
/// its default major, so every child command receives this label rather than
/// the raw argument.
#[derive(Clone, Copy, Default)]
pub(crate) enum PgMajor {
    Pg16,
    Pg17,
    /// Must match the `default` feature of every PostgreSQL-aware crate's
    /// `Cargo.toml`; nothing checks this, so change them together.
    #[default]
    Pg18,
}

impl PgMajor {
    /// Consume the optional leading `pgXX` argument. Any other argument stays
    /// in `args` for the subcommand to interpret.
    pub(crate) fn from_args(
        args: &mut Peekable<impl Iterator<Item = OsString>>,
    ) -> Self {
        match args.peek().and_then(|arg| Self::from_label(arg)) {
            Some(pg) => {
                args.next();
                pg
            }
            None => Self::default(),
        }
    }

    fn from_label(label: &OsStr) -> Option<Self> {
        match label.to_str()? {
            "pg16" => Some(Self::Pg16),
            "pg17" => Some(Self::Pg17),
            "pg18" => Some(Self::Pg18),
            _ => None,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Pg16 => "pg16",
            Self::Pg17 => "pg17",
            Self::Pg18 => "pg18",
        }
    }

    /// Read one path reported by `cargo pgrx info` for this major.
    fn pgrx_info(self, key: &str) -> Result<PathBuf, String> {
        let output = cargo_pgrx()
            .arg("info")
            .arg(key)
            .arg(self.label())
            .output()
            .map_err(|error| {
                format!("failed to run cargo pgrx info {key}: {error}")
            })?;

        if !output.status.success() {
            return Err(format!(
                "cargo pgrx info {key} failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(PathBuf::from(
            String::from_utf8_lossy(&output.stdout).trim(),
        ))
    }
}

/// pgrx-managed PostgreSQL installation targeted by one xtask invocation.
pub(crate) struct PgInstallation {
    major: PgMajor,
    pg_config: PathBuf,
}

impl PgInstallation {
    pub(crate) fn resolve(major: PgMajor) -> Result<Self, String> {
        let pg_config = major.pgrx_info("pg-config")?;
        Ok(Self { major, pg_config })
    }

    pub(crate) const fn label(&self) -> &'static str {
        self.major.label()
    }

    pub(crate) fn pg_config(&self) -> &Path {
        &self.pg_config
    }

    /// Read one path reported by this installation's `pg_config`.
    pub(crate) fn config_value(&self, arg: &str) -> Result<PathBuf, String> {
        let pg_config = self.pg_config.display();
        let output = Command::new(&self.pg_config)
            .arg(arg)
            .output()
            .map_err(|error| format!("failed to run {pg_config} {arg}: {error}"))?;

        if !output.status.success() {
            return Err(format!(
                "{pg_config} {arg} failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(PathBuf::from(
            String::from_utf8_lossy(&output.stdout).trim(),
        ))
    }

    pub(crate) fn install(&self, extension: Extension) -> Result<(), String> {
        run_command(
            cargo_pgrx()
                .arg("install")
                .arg("--package")
                .arg(extension.package())
                .arg("--pg-config")
                .arg(&self.pg_config),
        )
    }

    pub(crate) fn install_pg_test(&self, extension: Extension) -> Result<(), String> {
        run_command(
            cargo_pgrx()
                .arg("install")
                .arg("--package")
                .arg(extension.package())
                .arg("--features")
                .arg("pg_test")
                .arg("--pg-config")
                .arg(&self.pg_config),
        )
    }

    pub(crate) fn test(&self, extension: Extension) -> Result<(), String> {
        run_command(
            cargo_pgrx()
                .arg("test")
                .arg(self.label())
                .arg("--package")
                .arg(extension.package()),
        )
    }

    pub(crate) fn prepare_injection_points(&self) -> Result<(), String> {
        // PostgreSQL 17 introduced injection points.
        if matches!(self.major, PgMajor::Pg16) {
            return Ok(());
        }

        let label = self.label();

        let server_include = self.config_value("--includedir-server")?;
        let config_header = server_include.join("pg_config.h");
        let config = fs::read_to_string(&config_header).map_err(|error| {
            format!(
                "failed to read PostgreSQL configuration {}: {error}",
                config_header.display()
            )
        })?;
        if !config
            .lines()
            .any(|line| line.trim() == "#define USE_INJECTION_POINTS 1")
        {
            return Err(format!(
                "{label} at {} was built without injection-point support.\n\
                 Rebuild the pgrx-managed server with:\n  \
                 cargo pgrx init --{label}=download --configure-flag=--enable-injection-points",
                self.pg_config.display()
            ));
        }

        let install_root = self.major.pgrx_info("path")?;
        let source_root = install_root.parent().ok_or_else(|| {
            format!(
                "pgrx PostgreSQL install path has no source-tree parent: {}",
                install_root.display()
            )
        })?;
        let module_dir = source_root.join("src/test/modules/injection_points");
        if !module_dir.join("Makefile").is_file() {
            return Err(format!(
                "PostgreSQL injection-points test module not found at {}.\n\
                 Register a pgrx-managed PostgreSQL source build with --{label}=download.",
                module_dir.display()
            ));
        }

        println!(
            "Installing PostgreSQL injection_points test extension from {}",
            module_dir.display()
        );
        run_command(
            Command::new("make")
                .arg("-C")
                .arg(module_dir)
                .arg("USE_PGXS=1")
                .arg(format!("PG_CONFIG={}", self.pg_config.display()))
                .arg("install"),
        )
    }
}
