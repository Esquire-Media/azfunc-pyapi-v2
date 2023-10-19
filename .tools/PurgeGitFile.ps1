<#
.SYNOPSIS
This script removes a specified file from a git repository's history.

.DESCRIPTION
The script uses a series of git commands to filter out a specific file from the git history.
It then cleans up any leftover references and optimizes the local repository.
Finally, it force pushes the changes to the origin repository.

.PARAMETER FilePath
The path to the file you want to remove from the git history.

.EXAMPLE
.tools\PurgeGitFile.ps1 -FilePath "path/to/your/file.txt"

.NOTES
File Name      : PurgeGitFile.ps1
Author         : Isaac Jessup
Prerequisite   : Git must be installed and available in the system's PATH.
Date           : 2023/10/19
Version        : 1.0
#>

param (
    [Parameter(Mandatory = $true, HelpMessage = "The path to the file you want to remove from the git history.")]
    [string]$FilePath
)

# Ensure git is available
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Error "Git is not available on this system."
    exit 1
}

# Use git's filter-branch command to remove the file from the entire git history.
# This command goes through the entire commit history and removes the specified file.
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch $FilePath" --prune-empty --tag-name-filter cat -- --all

# After the above operation, git creates backup refs under refs/original/.
# This command lists all such refs and deletes them.
git for-each-ref --format='%(refname)' refs/original/ | ForEach-Object { git update-ref -d $_ }

# Clean up unnecessary files and optimize the local repository.
# This command collects and removes objects that are no longer reachable in your repository.
git gc --prune=now

# An aggressive garbage collection command that more thoroughly optimizes the repository.
# This can take some time, especially on large repositories.
git gc --aggressive --prune=now

# Force push the changes to all branches on the remote repository.
# This replaces the history on the remote with the local history.
git push origin --force --all

# Force push any tags to the remote repository.
git push origin --force --tags
