"""
Rule test code for unit testing of rules generated with Snakemake 9.16.4.dev3.
"""


import os
import sys
import shutil
import tempfile
from pathlib import Path
from subprocess import check_output

sys.path.insert(0, os.path.dirname(__file__))


def test_taxon_metadmg_damage(conda_prefix):

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir) / "workdir"
        config_path = Path(".tests/unit/taxon_metadmg_damage/config")
        data_path = Path(".tests/unit/taxon_metadmg_damage/data")
        expected_path = Path(".tests/unit/taxon_metadmg_damage/expected")

        # Copy config to the temporary workdir.
        shutil.copytree(config_path, workdir)

        # Copy data to the temporary workdir.
        shutil.copytree(data_path, workdir, dirs_exist_ok=True)

        # Run the test job.
        check_output(
            [
                "python",
                "-m",
                "snakemake",
                "results/metadmg/damage/HD827sonic_1_lib1_collapsed.bdamage.gz",
                "results/metadmg/damage/HD827sonic_1_lib1_collapsed.res.gz",
                "stats/metadmg/damage/HD827sonic_1_lib1_collapsed.stat.gz",
                "stats/metadmg/damage/HD827sonic_1_lib1_collapsed.rlens.gz",
                "--snakefile",
                "../../workflow/Snakefile",
                "-f",
                "--notemp",
                "--show-failed-logs",
                "-j1",
                "--target-files-omit-workdir-adjustment",
                "--allowed-rules",
                "taxon_metadmg_damage",
                "--configfile",
                "config/config.yaml",
                "--software-deployment-method",
                "conda",
                "--directory",
                workdir,
            ]
            + conda_prefix
        )

        # Check the output byte by byte using cmp/zmp/bzcmp/xzcmp.
        # To modify this behavior, you can inherit from common.OutputChecker in here
        # and overwrite the method `compare_files(generated_file, expected_file), 
        # also see common.py.
        import common
        common.OutputChecker(data_path, expected_path, workdir).check()
