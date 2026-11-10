//! The window: the standard shell around the generated admin frontend, one view per data type.
//!
//! Pattern: Facade — builds the descriptions, backends and sessions and hands them to `admin_window`. No drawing
//! code and no look of its own (ADR 0065, 0066).

use std::sync::Arc;

use fs_admin_frontend::view::AdminSurface;
use fs_admin_frontend::window::{TypeView, admin_window};
use fs_admin_frontend::{Backend, Session, SystemClock};
use fs_components_app::{AppError, AppWidget, WindowOptions, launch};
use fs_components_fields::{Look, Say};
use fs_components_styles::StyleId;
use fs_streaming_core::{Stream, Target};
use fs_types::{DescribesType, TypeDescription};

use crate::about::{HELP_URL, about};
use crate::args::{HELP, parse};
use crate::core_link::CoreLink;
use crate::open::{Program, open_program};
use crate::streams::StreamBackend;
use crate::targets::TargetBackend;
use crate::words::{TITLE, Words};

fn view(description: TypeDescription, backend: Arc<dyn Backend>, say: &Arc<dyn Say>) -> TypeView {
    let (id, label) = (description.name.clone(), description.label.clone());
    let session = Session::new(description, backend, Arc::new(SystemClock::new()));
    let look = Look::new(StyleId::Admin, false);
    TypeView {
        id,
        label,
        surface: Arc::new(AdminSurface::new(session, look, Arc::clone(say), None)),
    }
}

/// The window over a running program.
///
/// # Errors
/// [`AppError`] if the shell cannot be built.
pub fn program_window(program: &Program) -> Result<AppWidget, AppError> {
    let core = CoreLink::new(program.streams(), program.handle());
    let say: Arc<dyn Say> = Arc::new(Words);
    let views = vec![
        view(
            Stream::describe(),
            Arc::new(StreamBackend::new(core.clone())),
            &say,
        ),
        view(Target::describe(), Arc::new(TargetBackend::new(core)), &say),
    ];
    admin_window(TITLE, &say, views, about(), HELP_URL.into())
}

/// The program as a command: the exit code.
pub fn app_command(words: &[String]) -> i32 {
    match run(words) {
        Ok(()) => 0,
        Err(m) => {
            eprintln!("fs-streaming: {m}");
            1
        }
    }
}

fn run(words: &[String]) -> Result<(), String> {
    let args = parse(words)?;
    if args.help {
        println!("{HELP}");
        return Ok(());
    }
    let program = open_program(&args)?;
    let app = program_window(&program).map_err(|e| e.to_string())?;
    let (width, height) = args.size;
    if let Some(file) = &args.snapshot {
        app.picture(width, height, file)
            .map_err(|e| e.to_string())?;
        eprintln!("wrote {}", file.display());
        return Ok(());
    }
    let mut options = WindowOptions::default();
    options.width = width;
    options.height = height;
    let result = launch(options, app).map_err(|e| e.to_string());
    program.shutdown();
    result
}
