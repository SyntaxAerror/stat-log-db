import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


# region Global Variables

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "tools.sh"
VENV_TEST = ROOT / ".venv_test"
PACKAGE_NAME = "stat-log-db"

GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

# endregion


# region testing tools


def _ensure_test_venv():
    """Ensure the test virtual environment is created."""
    if not VENV_TEST.exists():
        subprocess.run([sys.executable, "-m", "venv", str(VENV_TEST)], check=True)


def _venv_python():
    """Return path to the virtual environment's python interpreter."""
    return VENV_TEST / ("Scripts" if os.name == "nt" else "bin") / ("python.exe" if os.name == "nt" else "python")


def is_installed(package: str) -> bool:
    """
    Check if a package is installed in the test virtual environment using 'pip show'.
    Assumes the test venv has been created.
    """
    _ensure_test_venv()
    python_executable = _venv_python()
    result = subprocess.run([str(python_executable), "-m", "pip", "show", package], capture_output=True, text=True)
    return result.returncode == 0


def _find_bash_executable(): # TODO: Improve this
    """Find bash executable, preferring Git Bash on Windows."""
    if os.name != "nt":
        return "bash"
    # Common Git Bash locations on Windows
    common_paths = [
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files (x86)\Git\bin\bash.exe",
        r"C:\Users\{}\AppData\Local\Programs\Git\bin\bash.exe".format(os.getenv("USERNAME", "")),
        r"C:\Git\bin\bash.exe",
    ]
    # Check common paths first
    for path in common_paths:
        if os.path.isfile(path):
            return path
    # Try to find bash using 'where' command
    try:
        result = subprocess.run(["where", "bash"], capture_output=True, text=True, check=True)
        bash_path = result.stdout.strip().split('\n')[0]  # Get first result
        if os.path.isfile(bash_path):
            return bash_path
    except (subprocess.CalledProcessError, FileNotFoundError, IndexError):
        pass
    # If we get here, bash was not found
    raise FileNotFoundError(
        "Git Bash not found. Please install Git for Windows from https://git-scm.com/download/win "
        "or ensure bash.exe is in your PATH. Tried the following locations:\n" +
        "\n".join(f"  - {path}" for path in common_paths)
    )


def run_tools(args, use_test_venv=False):
    """Run tools.sh returning (code, stdout+stderr)."""
    env = os.environ.copy()
    if use_test_venv:
        _ensure_test_venv()
        scripts_dir = VENV_TEST / ("Scripts" if os.name == "nt" else "bin")
        env["PATH"] = str(scripts_dir) + os.pathsep + env.get("PATH", "")
        env["VIRTUAL_ENV"] = str(VENV_TEST)
        env["PYTHONHOME"] = ""  # ensure venv python resolution
    bash = _find_bash_executable()
    proc = subprocess.run([bash, str(SCRIPT), *args], capture_output=True, text=True, cwd=ROOT, env=env)
    return proc.returncode, proc.stdout + proc.stderr


# endregion


@pytest.fixture()  # scope="module"
def test_venv():
    """
    Provision an isolated virtual environment used for install/uninstall tests.
    The directory is removed after all related tests complete.
    """
    _ensure_test_venv()
    yield VENV_TEST
    # Teardown: remove the virtual environment directory
    if VENV_TEST.exists():
        shutil.rmtree(VENV_TEST)


def test_help():
    code, out = run_tools(["-h"])
    assert code == 0
    # Read README.md
    readme_path = ROOT / "README.md"
    assert readme_path.exists(), f"README not found at {readme_path}"
    readme_content = None
    with open(readme_path, "r", encoding="utf-8") as f:
        readme_content = f.read().strip()
    assert not (readme_content is None), "Unable to read README"
    # Compare README content with help output
    try:
        assert out == readme_content, "Help output does not match README content"
    except AssertionError:
        assert out.strip() == readme_content.strip(), "Help output does not match README content (leading & trailing whitespace stripped)"


@pytest.mark.skipif(GITHUB_ACTIONS, reason="Skipping test on GitHub Actions")
def test_install_dev(test_venv):
    code, out = run_tools(["-id"], use_test_venv=True)
    assert code == 0
    assert "Installing" in out
    assert "dev" in out
    assert is_installed(PACKAGE_NAME), "Package should be installed after dev install"


@pytest.mark.skipif(GITHUB_ACTIONS, reason="Skipping test on GitHub Actions")
def test_install_normal(test_venv):
    code, out = run_tools(["-in"], use_test_venv=True)
    assert code == 0
    assert "Installing" in out
    assert "dev" not in out
    assert is_installed(PACKAGE_NAME), "Package should be installed after normal install"


def test_install_invalid_arg(test_venv):
    code, out = run_tools(["-ix"], use_test_venv=True)
    assert code == 1
    assert ("Unsupported argument" in out) or ("Invalid install mode" in out)
    assert not is_installed(PACKAGE_NAME), "Package should not be installed after invalid install argument"


@pytest.mark.skipif(GITHUB_ACTIONS, reason="Skipping test on GitHub Actions")
def test_uninstall(test_venv):
    # Ensure something installed first (dev mode)
    icode, iout = run_tools(["-id"], use_test_venv=True)
    assert icode == 0
    assert is_installed(PACKAGE_NAME), "Package should be installed (before uninstall)"
    ucode, uout = run_tools(["-u"], use_test_venv=True)
    assert ucode == 0
    assert "Uninstalling" in uout
    assert "Uninstall complete" in uout
    assert not is_installed(PACKAGE_NAME), "Package should not be installed after uninstall"


@pytest.mark.skipif(GITHUB_ACTIONS, reason="Skipping test on GitHub Actions")
def test_install_and_clean_multi_flag(test_venv):
    code, out = run_tools(["-id", "-c"], use_test_venv=True)
    assert code == 0
    assert is_installed(PACKAGE_NAME), "Package should be installed"
    assert "Installing" in out
    assert "Cleaning up workspace" in out
    assert "Cleanup complete" in out
    assert is_installed(PACKAGE_NAME), "Cleanup should not remove installed package"


def test_test_no_arg():
    code, out = run_tools(["-t"])
    assert code == 1
    try:
        assert out == "Option -t requires an argument"
    except AssertionError:
        assert out.strip() == "Option -t requires an argument"


def test_test_invalid_arg():
    code, out = run_tools(["-tx"])
    assert code == 1
    assert ("Unsupported argument" in out) or ("Invalid test mode" in out)


@pytest.mark.skipif(GITHUB_ACTIONS, reason="Skipping test on GitHub Actions")
def test_clean():
    code, out = run_tools(["-c"])
    assert code == 0
    assert "Cleaning up workspace" in out
    assert "Cleanup complete" in out
