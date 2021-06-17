import os
import pathlib
import shutil
from subprocess import PIPE, check_call, check_output

import click
import yaml


def clean_wd():
    _, dirnames, filenames = next(os.walk("."))
    for d in dirnames:
        if not d.startswith(".git"):
            shutil.rmtree(d)
    for f in filenames:
        if not f.startswith(".git"):
            os.unlink(f)


def print_ids():
    revid = check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    ts, msg = (
        check_output(["git", "log", "-1", '--format="%at %s"'])
        .decode()
        .strip()[1:-1]
        .split()
    )
    print(f"{ts}.0 {revid} {msg}")
    print(f"{msg:<5} | {'':>5} | {'':>20} | R {revid} | {ts}.0")

    for currentpath, dirnames, filenames in os.walk("."):
        if currentpath == ".":
            output = check_output(["git", "cat-file", "-p", "HEAD"]).decode()
            dirhash = output.splitlines()[0].split()[1]
        else:
            currentpath = currentpath[2:]
            output = check_output(["git", "ls-tree", "HEAD", currentpath]).decode()
            dirhash = output.split()[2]

        print(f"{'':>5} | {'':>5} | {currentpath:<20} | D {dirhash} | 0.0")
        for fname in filenames:
            fname = os.path.join(currentpath, fname)
            output = check_output(["git", "ls-tree", "HEAD", fname]).decode()
            fhash = output.split()[2]
            print(f"{'':>5} | {'':>5} | {fname:<20} | C {fhash} | 0.0")

        if ".git" in dirnames:
            dirnames.remove(".git")


def generate_repo(repo_desc, output_dir):
    check_call(["git", "init", output_dir], stdout=PIPE, stderr=PIPE)
    os.chdir(output_dir)
    os.environ.update(
        {
            "GIT_AUTHOR_NAME": "SWH",
            "GIT_AUTHOR_EMAIL": "contact@softwareheritage.org",
            "GIT_COMMITTER_NAME": "SWH",
            "GIT_COMMITTER_EMAIL": "contact@softwareheritage.org",
        }
    )

    for rev_d in repo_desc:
        parents = rev_d.get("parents")
        if parents:
            # move at the proper (first) parent position, if any
            check_call(["git", "checkout", parents[0]], stdout=PIPE)

        # give a branch name (the msg) to each commit to make it esier to
        # navigate in history
        check_call(["git", "checkout", "-b", rev_d["msg"]], stdout=PIPE)

        if parents and len(parents) > 1:
            # it's a merge
            check_call(["git", "merge", "--no-commit", *parents[1:]], stdout=PIPE)

        clean_wd()
        for path, content in rev_d["content"].items():
            p = pathlib.Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
        os.environ.update(
            {
                "GIT_AUTHOR_DATE": str(rev_d["date"]),
                "GIT_COMMITTER_DATE": str(rev_d["date"]),
            }
        )
        check_call(["git", "add", "."], stdout=PIPE)
        check_call(
            [
                "git",
                "commit",
                "--all",
                "--allow-empty",
                "-m",
                rev_d["msg"],
            ],
            stdout=PIPE,
        )
        print_ids()


@click.command(name="generate-repo")
@click.argument("input-file")
@click.argument("output-dir")
@click.option("-C", "--clean-output/--no-clean-output", default=False)
def main(input_file, output_dir, clean_output):
    repo_desc = yaml.load(open(input_file))
    if clean_output and os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    generate_repo(repo_desc, output_dir)


if __name__ == "__main__":
    main()
