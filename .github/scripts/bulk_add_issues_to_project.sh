#!/usr/bin/env bash

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
