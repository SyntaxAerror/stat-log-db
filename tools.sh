#! /bin/bash

# Flags / state
supported_installation_opts="d n"
install=""
uninstall=0
clean=0
supported_doc_opts="h s"
doc=""
supported_test_opts="p t a d s"
test=""

while getopts ":i:t:d:chu" flag; do
    case "${flag}" in
        i) if [[ " $supported_installation_opts " =~ " $OPTARG " ]]; then
                install="$OPTARG"
            else
                echo "Unsupported argument '$OPTARG' for '-$flag'. Please specify one of: $supported_installation_opts" >&2 && exit 1;
            fi
        ;;
        u) uninstall=1;;
        c) clean=1;;
        t) if [[ " $supported_test_opts " =~ " $OPTARG " ]]; then
                test="$OPTARG"
            else
                echo "Unsupported argument '$OPTARG' for '-$flag'. Please specify one of: $supported_test_opts" >&2 && exit 1;
            fi
        ;;
        d) if [[ " $supported_doc_opts " =~ " $OPTARG " ]]; then
                doc="$OPTARG"
            else
                echo "Unsupported argument '$OPTARG' for '-$flag'. Please specify one of: $supported_doc_opts" >&2 && exit 1;
            fi
        ;;
        h) cat README.md && exit 0;;
        :)
            echo "Option -$OPTARG requires an argument" >&2; exit 1;;
        ?) echo "Unknown option -$OPTARG" >&2; exit 1;;
    esac
done

# Install [-i]
if [ -n "$install" ]; then
    case "$install" in
        d)
            echo "Installing (editable with dev extras)..."
            python -m pip install -e ./stat_log_db[dev]
            ;;
        n)
            echo "Installing..."
            python -m pip install ./stat_log_db
            ;;
        *)
            echo "Invalid install mode '$install'. Use one of: $supported_installation_opts" >&2
            exit 1
            ;;
    esac
fi

# Run tests [-t]
if [ -n "$test" ]; then
    case "$test" in
        d)
            echo "Running stat_log_db tests..."
            pytest ./stat_log_db/tests/
            ;;
        t)
            echo "Running tools.sh tests..."
            pytest ./tests/test_tools.py
            ;;
        p)
            echo "Running project tests..."
            pytest ./tests/
            ;;
        a)
            echo "Running all tests..."
            pytest
            ;;
        s)
            echo "Running style tests (flake8)..."
            flake8 .
            # flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
            ;;
        *)
            echo "Invalid test mode '$test'. Use one of: $supported_test_opts" >&2
            exit 1
            ;;
    esac
fi

# Documentation [-d]
if [ -n "$doc" ]; then
    case "$doc" in
        h)
            echo "Generating HTML documentation..."
            if [ ! -d "stat_log_db/docs" ]; then
                mkdir -p stat_log_db/docs
            fi
            pdoc --output-dir stat_log_db/docs stat_log_db
            ;;
        s)
            echo "Hosting documentation..."
            pdoc stat_log_db --host localhost
            ;;
        *)
            echo "Invalid doc mode '$doc'. Use one of: $supported_doc_opts" >&2
            exit 1
            ;;
    esac
fi

# Clean artifacts [-c]
if [ "$clean" -eq 1 ]; then
    echo "Cleaning up workspace..."
    static_dirs=(
        ".pytest_cache"
        "stat_log_db/build"
        "stat_log_db/dist"
        "stat_log_db/src/stat_log_db.egg-info"
    )
    rm -rf "${static_dirs[@]}"
    # Recursively find and remove all __pycache__ directories
    find . -type d -name '__pycache__' -exec rm -rf {} +
    echo "Cleanup complete."
fi

# Uninstall [-u]
if [ $uninstall -eq 1 ]; then
    echo "Uninstalling..."
    python -m pip uninstall -y stat_log_db
    echo "Uninstall complete."
fi
