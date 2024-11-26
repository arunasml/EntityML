#!/usr/bin/zsh

# Supports zsh and bash
SCRIPT_ABS_PATH="$( cd "$( dirname "${BASH_SOURCE[0]:-$0}" )" >/dev/null 2>&1 && pwd )"

# Output logging path must be writable and support creation of symlinks
export COMPOSE_LOG_PATH=~/log/compose