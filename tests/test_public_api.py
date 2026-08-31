import os
import subprocess
import sys

import object_streams


# Modules that reach the database or a transport must stay off the package
# root so `import object_streams` works before django.setup().
DEFERRED_MODULES = (
    "object_streams.models",
    "object_streams.outbox",
    "object_streams.postgres",
    "object_streams.producers",
    "object_streams.retention",
    "object_streams.sessions",
    "object_streams.transports.channels",
)


def test_every_exported_name_resolves():
    assert [name for name in object_streams.__all__ if not hasattr(object_streams, name)] == []


def test_exports_are_sorted_and_unique():
    assert list(object_streams.__all__) == sorted(set(object_streams.__all__))


def test_package_imports_without_django_setup():
    script = "\n".join(
        [
            "import sys",
            "import object_streams",
            f"deferred = {DEFERRED_MODULES!r}",
            "loaded = [name for name in deferred if name in sys.modules]",
            "assert not loaded, loaded",
            "for name in object_streams.__all__:",
            "    getattr(object_streams, name)",
            "print(object_streams.__version__)",
        ]
    )
    env = {key: value for key, value in os.environ.items() if key != "DJANGO_SETTINGS_MODULE"}

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == object_streams.__version__
