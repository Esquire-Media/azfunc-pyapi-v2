#!/bin/bash

# Script: PurgeGitFile.sh
# Description: This script removes a specified file from a git repository's history.
# Usage: .tools/PurgeGitFile.sh <path-to-your-file>
# Author: Isaac Jessup
# Date: 2023/10/19

# Default values for arguments
FILE_PATH=""

# Parse named arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
    --FilePath)
        FILE_PATH="$2"
        shift
        ;;
    *)
        echo "Unknown parameter passed: $1"
        exit 1
        ;;
    esac
    shift
done

# Check if git is available on the system.
if ! command -v git &>/dev/null; then
    echo "Error: Git is not available on this system."
    exit 1
fi

# Check if a file path was provided.
if [[ -z "$FILE_PATH" ]]; then
    echo "Usage: $0 --FilePath <path-to-your-file>"
    exit 1
fi

# Use git's filter-branch command to remove the file from the entire git history.
# This command goes through the entire commit history and removes the specified file.
git filter-branch --force --index-filter "git rm --cached --ignore-unmatch $FILE_PATH" --prune-empty --tag-name-filter cat -- --all

# After the above operation, git creates backup refs under refs/original/.
# This command lists all such refs and deletes them.
git for-each-ref --format="%(refname)" refs/original/ | xargs -I {} git update-ref -d {}

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
