import os
from pathlib import Path
from typing import List


def get_all_files_in_dir(input_dir: Path) -> List[Path]:
    """
    Retrieve all files in a given directory.

    This function iterates over the contents of the specified directory and returns
    a list of file paths for all regular files (excluding subdirectories).

    Parameters:
    -----------
    input_dir : Path
        The directory whose files need to be listed.

    Returns:
    --------
    List[Path]
        A list containing the full paths of all files in the directory.

    Example:
    --------
    >>> from pathlib import Path
    >>> get_all_files_in_dir(Path("/some/directory"))
    [PosixPath('/some/directory/file1.txt'), PosixPath('/some/directory/file2.csv')]

    Notes:
    ------
    - Hidden files (e.g., `.gitignore`) are included in the list.
    - Subdirectories are ignored; only regular files are returned.
    """
    output = []
    for fp in os.listdir(input_dir):
        full_path = input_dir / fp
        if full_path.is_file():
            output.append(full_path)
    return output
