import hashlib
import json
import os
import pathlib
import shutil
import tarfile

APP_NAME = "codex-proxy-rs"

def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

version = os.environ["VERSION"]
platforms = json.loads(os.environ["PLATFORM_MATRIX"])["include"]
frontend_dir = pathlib.Path("dist/frontend")
binary_artifacts_dir = pathlib.Path("dist/binaries")
release_dir = pathlib.Path("dist/release")
package_dir = pathlib.Path("package")

shutil.rmtree(release_dir, ignore_errors=True)
release_dir.mkdir(parents=True)
config_template = pathlib.Path("deploy/config.example.yaml")
shutil.copy2(config_template, release_dir / config_template.name)
compose = pathlib.Path("deploy/compose.yaml").read_text()
image = f"{os.environ['REGISTRY']}/{os.environ['IMAGE_NAME']}"
default_image = "${CPR_IMAGE:-" + image + ":latest}"
if compose.count(default_image) != 1:
    raise SystemExit("Expected exactly one default application image in deploy/compose.yaml")
# 发布附件固定镜像版本，防止后续 pull 与随包配置模板脱节。
versioned_image = "${CPR_IMAGE:-" + f"{image}:{os.environ['DOCKER_VERSION']}" + "}"
compose = compose.replace(default_image, versioned_image)
(release_dir / "compose.yaml").write_text(compose)
assets = [release_dir / config_template.name, release_dir / "compose.yaml"]

for platform in platforms:
    platform_name = f"{platform['os']}-{platform['arch']}"
    artifact_name = f"{APP_NAME}-{platform_name}"
    binary_src = binary_artifacts_dir / artifact_name / platform["binary"]
    if not binary_src.is_file():
        raise SystemExit(f"Missing binary artifact: {binary_src}")

    shutil.rmtree(package_dir, ignore_errors=True)
    package_root = package_dir / APP_NAME
    web_dist = package_root / "web" / "dist"
    web_dist.mkdir(parents=True)

    binary_dest = package_root / platform["binary"]
    shutil.copy2(binary_src, binary_dest)
    binary_dest.chmod(0o755)
    cli_src = binary_src.parent / "cpr-plugin"
    cli_dest = package_root / "cpr-plugin"
    shutil.copy2(cli_src, cli_dest)
    cli_dest.chmod(0o755)
    plugin_manifest = json.loads(pathlib.Path("backend/apps/shared-budget-plugin/plugin.json").read_text())
    plugin_name = f"{plugin_manifest['publisher']}.{plugin_manifest['name']}-{plugin_manifest['version']}-{platform['target']}.tar.gz"
    plugin_src = binary_src.parent / "plugins" / plugin_name
    plugin_dest = release_dir / plugin_name
    shutil.copy2(plugin_src, plugin_dest)
    assets.append(plugin_dest)
    shutil.copytree(frontend_dir, web_dist, dirs_exist_ok=True)
    deploy_dir = package_root / "deploy"
    deploy_dir.mkdir()
    shutil.copy2(config_template, deploy_dir / config_template.name)

    archive_name = f"{APP_NAME}_{version}_{platform['os']}_{platform['arch']}.{platform['archive']}"
    archive_path = release_dir / archive_name
    if platform["archive"] == "tar.gz":
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(package_root, arcname=APP_NAME)
    else:
        raise SystemExit(f"Unsupported archive format: {platform['archive']}")
    assets.append(archive_path)

checksum_path = release_dir / "checksums.txt"
with checksum_path.open("w", encoding="utf-8") as file:
    for asset in assets:
        file.write(f"{sha256(asset)}  {asset.name}\n")
