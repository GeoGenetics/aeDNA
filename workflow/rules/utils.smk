import os
import pandas as pd
from typing import List, Dict
from collections import defaultdict
from snakemake.io import Namedlist



###############
### CLASSES ###
###############

# Implementation of switch/case in Python
# https://code.activestate.com/recipes/410692/

class switch(object):
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration
    
    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args: # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False


#################
### FUNCTIONS ###
#################

### General

def flatten(
        list_of_lists: List
) -> List:
        """ Flatten a list of lists recursively

        https://stackoverflow.com/a/53778278

        :param list_of_lists: A list of lists
        :return result: A string that has been flattened from a list of lists
        """
        result = list()
        for i in list_of_lists:
                if isinstance(i, list):
                        result.extend(flatten(i))
                else:
                        result.append(str(i))
        return result


def format_partial(
    string: str,
    **wildcards: str
) -> str:
    """ Format string while ignoring missing string argument

    https://stackoverflow.com/a/43526674

    :param string: String to be formatted
    :param **wildcards: Optional strings to be formatted
    :return string: Partially formatted string
    """
    class SafeDict(dict):
        def __missing__(self, key):
            return '{' + key + '}'

    return str(string).format_map(SafeDict(**wildcards))



def expand_pandas(
    string: List,
    df: pd.DataFrame,
    allow_missing = False
) -> List:
    """ Expand string following columns in the dataframe
    """
    return set(flatten([expand(string, **row._asdict(), allow_missing=allow_missing) for row in df.itertuples(False)]))



def tuple2namedlist(
    tuple: tuple
) -> Namedlist:
    """ Convert a tuple (e.g.) from df.itertuples() into a
    Namedlist (similar to snakemake's wildcards)
    """
    return Namedlist(fromdict = tuple._asdict())



def to_dict(
    keys: List,
    values: List
) -> Dict:
    """Convert two lists (keys and values) to a dictionary

    Even though it is a simple function, it makes the code more readable.
    """
    if len(keys) != len(values):
        raise ValueError("")

    out_dict = defaultdict(list)
    for key, val in list(zip(keys, values)):
        out_dict[key].append(val)

    return out_dict



def create_symlink(
    source: str,
    dest: str,
    force: bool = False
):
    """ Create symlink from "source" to "dest" with relative path
    """
    source = Path(source)
    dest = Path(dest)
    if dest.exists() and dest.is_symlink() and force:
        dest.unlink()
    dest.symlink_to(Path(os.path.relpath(source.parent, dest.parent)).joinpath(source.name))



def allow_remote(url, keep_local = True):
    # HTTP(s)
    url, n = re.subn(r"^https?://", "", url)
    if n == 1:
        from snakemake.remote.HTTP import RemoteProvider
        return RemoteProvider().remote(url, keep_local=keep_local)

    # FTP(s)
    url, n = re.subn(r"^ftps?://", "", url)
    if n == 1:
        from snakemake.remote.FTP import RemoteProvider
        return RemoteProvider().remote(url, keep_local=keep_local)

    # If a file URL, just return it
    return url


def expand_ext(path, ext):
    if isinstance(path, list):
        l = [expand(Path(p).with_suffix(".{ext}"), ext=ext, allow_missing=True) for p in path]
        l = list(map(list, zip(*l)))
        return flatten(l)
    elif isinstance(path, str):
        return expand(Path(path).with_suffix(".{ext}"), ext=ext, allow_missing=True)


def ext_dict(path, keys = None):
    if not keys:
        # Extract extensions
        ext = ["".join(Path(p).suffixes) for p in path]
        # Fix extensions to be used as keys
        keys = [e.replace(".gz", "").split(".")[-1] for e in ext]
    return to_dict(keys, path)



def get_tmp(
    large: bool = False
) -> str:
    """ Returns path to temporary folder

    :param large: large temp folder (NFS), instead of local (e.g. /tmp/)
    :return path: a string with path to temp folder
    """
    import tempfile

    if large:
        path = Path("temp") / "large_temp"
    else:
        if "PBS_JOBID" in os.environ:
            path = Path("/scratch/$PBS_JOBID")
        elif "SLURM_JOB_ID" in os.environ and "SLURM_TMPDIR" in os.environ:
            path = Path("$SLURM_TMPDIR")
        else:
            path = tempfile.tempdir

    return(str(path) + "/")



def check_cmd(
    cmd: str,
    mandatory_args: List = None,
    forbidden_args: List = None
) -> str:
    """ Check command for the presence of certain mandatory and/or forbidden arguments
    """
    if mandatory_args:
        found_args = set(mandatory_args) - set(cmd.replace("="," ").split(" "))
        if found_args:
            raise ValueError(f"Argument(s) {found_args} are mandatory!")

    if forbidden_args:
        found_args = set(cmd.replace("="," ").split(" ")).intersection(forbidden_args)
        if found_args:
            raise ValueError(f"Argument(s) {found_args} not allowed!")

    return cmd


### Config

def _item_or_sample(row, item):
    i = getattr(row, item, None)
    if pd.isnull(i):
        return getattr(row, "sample")
    return i

def is_activated(xpath):
    c = config
    for entry in xpath.split("/"):
        c = c.get(entry, {})
    return bool(c.get("activate", False))



### Samples

def get_rule_stats(rule_name):
    r = re.compile("^stats/")
    return set(filter(r.match, getattr(rules, rule_name).output))

def get_samples(type=".", material=".", all=True):
    # Get state where all samples are TRUE
    bool_true = units["sample"].str.match(".")

    type_cond = units.type.str.match(type, case=False) if "type" in units.columns else bool_true
    material_cond = units.material.str.match(material, case=False) if "material" in units.columns else bool_true
    tot_cond = (type_cond & material_cond).groupby(level="sample")
    if all:
        tot_cond = tot_cond.all()

    return [idx for idx, bool in tot_cond.items() if bool]

def get_groups():
    return samples["group"].unique()

def get_group_samples(group):
    return samples.loc[samples["group"] == group]["sample"]



### Units 

def get_libraries():
    return units["library"].unique()

def get_lanes():
    return units["lane"].unique()

def is_units_align(units):
    return units.data.str.endswith(".cram")

def get_units_pe(reverse = False):
    units_pe = units.type.isin(["pe", "hic"])

    if reverse:
        return units[~units_pe]
    else:
        return units[units_pe]

def get_units_se():
    return get_units_pe(reverse = True)

def get_adapters(wildcards):
    adapters = units.loc[(wildcards.sample, wildcards.library, wildcards.lane), "adapters"]
    if isinstance(adapters, str):
        return adapters.split(",")

    # If no adapters found, return None
    return None

def get_sample_libraries(sample):
    return units.loc[sample]["library"].unique()

def get_sample_library_lanes(sample, library):
    return units.loc[(sample, library)].lane.unique()

def get_sample_library_lane_data(sample, library, lane):
    return expand(units.loc[(sample, library, lane)].data, Read = ["1","2"])

def is_lane_pe(sample, library, lane):
    return get_units_pe().index.isin([(sample, library, lane)]).any()

def get_read_type_trim(read_type_map):
    if read_type_map == "pe":
        return ["R1","R2"]
    elif read_type_map == "se":
        return ["R"]
    else:
        return read_type_map

def get_read_type_map(sample, library, lane):
    read_type_map = list()

    if is_lane_pe(sample, library, lane):
        read_type_map.append("pe")
        if is_activated("reads/trim"):
            trimmer = config["reads"]["trim"]["tool"]

            for case in switch(trimmer):
                if case('adapterremoval'):
                    read_type_map.append("singleton")
                    if is_activated("reads/collapse"):
                        read_type_map.append("collapsed")
                        read_type_map.append("collapsedtrunc")
                    break
                if case('fastp'):
                    read_type_map.append("singleton")
                    if is_activated("reads/collapse"):
                        read_type_map.append("collapsed")
                    break
                if case('bbduk'):
                    read_type_map.append("singleton")
                    break
                if case('trimmomatic'):
                    read_type_map.append("singleton1")
                    read_type_map.append("singleton2")
                    break
                if case('cutadapt'):
                    break
                if case():
                    raise ValueError("Invalid trimmer provided!")
    else:
        read_type_map.append("se")

    return read_type_map
