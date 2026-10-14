use codex_proxy_rs::bootstrap::{Invocation, PluginOperation, RunOutput};
use std::{env, error::Error, io, process::ExitCode};

use provider_openai::ensure_rustls_provider;

fn main() -> Result<ExitCode, Box<dyn Error + Send + Sync>> {
    let Some(invocation) = arguments()? else {
        println!(
            "用法: codex-proxy-rs [serve]\n      codex-proxy-rs plugin <实例 ID> [命令] [参数]\n\n使用 --help 查看帮助；插件命令后单独使用 --help 查看该命令帮助。"
        );
        return Ok(ExitCode::SUCCESS);
    };
    ensure_rustls_provider();
    match runtime()?.block_on(codex_proxy_rs::bootstrap::run(invocation))? {
        RunOutput::Served => Ok(ExitCode::SUCCESS),
        RunOutput::Command(completion) => match completion.result {
            Some(Ok(output)) => {
                print!("{}", output.stdout);
                eprint!("{}", output.stderr);
                if completion.graph_drained {
                    Ok(ExitCode::from(output.exit_code))
                } else {
                    eprintln!(
                        "{}",
                        gateway_admin::model::plugins::PluginCommandError::Incomplete {
                            saved_accounts: output.saved_accounts,
                            reason: "原请求清理未在排空时限内完成",
                        }
                    );
                    Ok(ExitCode::FAILURE)
                }
            }
            Some(Err(error)) => {
                eprintln!("{error}");
                if !completion.graph_drained {
                    eprintln!("原请求清理未在排空时限内完成");
                }
                Ok(ExitCode::FAILURE)
            }
            None => {
                eprintln!("命令结束状态未确认，副作用结果未知；请核对后再决定是否重试");
                Ok(ExitCode::FAILURE)
            }
        },
    }
}

fn arguments() -> Result<Option<Invocation>, io::Error> {
    let arguments = env::args_os()
        .skip(1)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| invalid_cli("command arguments must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut args = arguments.into_iter();
    match args.next().as_deref() {
        None | Some("serve") => {
            reject_extra_arguments(args)?;
            Ok(Some(Invocation::Serve))
        }
        Some("--help" | "-h" | "help") => {
            reject_extra_arguments(args)?;
            Ok(None)
        }
        Some("plugin") => {
            let instance = args
                .next()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| invalid_cli("plugin requires an instance ID"))?;
            if instance == "--help" || instance == "-h" {
                reject_extra_arguments(args)?;
                return Ok(None);
            }
            let operation = match args.next() {
                None => PluginOperation::Help(None),
                Some(name) if name == "--help" || name == "-h" => {
                    reject_extra_arguments(args)?;
                    PluginOperation::Help(None)
                }
                Some(name) => {
                    let arguments: Vec<String> = args.collect();
                    if arguments.len() == 1 && matches!(arguments[0].as_str(), "--help" | "-h") {
                        PluginOperation::Help(Some(name))
                    } else {
                        PluginOperation::Execute { name, arguments }
                    }
                }
            };
            Ok(Some(Invocation::Plugin {
                instance,
                operation,
            }))
        }
        Some(_) => Err(invalid_cli("unknown command; expected serve or plugin")),
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

fn reject_extra_arguments(mut arguments: impl Iterator) -> Result<(), io::Error> {
    if arguments.next().is_some() {
        return Err(invalid_cli("unexpected extra command arguments"));
    }
    Ok(())
}

fn invalid_cli(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
