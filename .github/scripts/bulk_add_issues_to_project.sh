#!/usr/bin/env bash

# This script was created as a oneoff to add all issues with the 'area/platform'
# label to the Platform Scrum Github Project. It shouldn't need to be run again
# because a Github Workflow exists to automatically add new issues that have
# the 'area/platform' label.

set -e

PLATFORM_SCRUM_PROJECT_ID="PN_kwDOA4_XW84AAj7t"
PLATFORM_ISSUES=($(gh issue list -l "area/platform" -L 1000 -s all --json id | jq -r 'map(.id) | @sh' | tr -d \'))

for ISSUE in "${PLATFORM_ISSUES[@]}"
do
  gh api graphql -f query='
    mutation($project_id: String! $issue_id: String!) {
      addProjectNextItem(input: {projectId: $project_id contentId: $issue_id}) {
        projectNextItem {
          id
        }
      }
    }' -f project_id=$PLATFORM_SCRUM_PROJECT_ID -f issue_id=$ISSUE > /dev/null
done
