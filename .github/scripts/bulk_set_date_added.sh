#!/usr/bin/env bash

set -e

# internal id of 'Platform Scrum' project
PLATFORM_SCRUM_PROJECT_ID="PN_kwDOA4_XW84AAj7t"

# internal id of 'Date Added' project field
DATE_ADDED_FIELD_ID="MDE2OlByb2plY3ROZXh0RmllbGQxMzUyMzI5"

HAS_NEXT="true"
CURSOR=""
ISSUES_STRING=""

# fetch a page of issues with createdAt field included. uses cursor pagination, max page size is 100
function get_issues() {
  if [ -z "$CURSOR" ]; then cursorArg=''; else cursorArg="-f cursor=$CURSOR"; fi

  read -a OUTPUT < <(echo $(gh api graphql -f query='
    query($project_id: ID! $cursor: String) {
      node(id: $project_id) {
        ... on ProjectNext {
          items(first: 100, after: $cursor) {
            nodes {
              id
              content {
                ...on Issue {
                  createdAt
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
      }
    }' -f project_id=$PLATFORM_SCRUM_PROJECT_ID $cursorArg | jq -r '.data.node.items | .pageInfo.endCursor, .pageInfo.hasNextPage, ([.nodes[] | .id + "|" + .content.createdAt]|join(";"))
  '))

  CURSOR=${OUTPUT[0]}
  HAS_NEXT=${OUTPUT[1]}
  ISSUES_STRING+="${OUTPUT[2]};"
}

# while there is a next page, repeat API call append issues
while [ "$HAS_NEXT" = "true" ]
do
  echo "fetching another page with cursor $CURSOR"
  get_issues
done

# jq joined issues into a single string with ';' so now we split on ';'
IFS=';' read -r -a ISSUE_WITH_CREATED_AT_ARR <<< "$ISSUES_STRING"
echo "Fetched ${#ISSUE_WITH_CREATED_AT_ARR[@]} issues"

# update an issue in the project, setting the 'Date Added' field to createdAt
function update_issue() {
  echo "issue id: $1"
  echo "created at: $2"

  gh api graphql -f query='
    mutation($project_id: ID! $issue_id: String! $date_added_field_id: String! $created_at_value: Date!) {
      updateProjectNextItemField(
        input: {
          projectId: $project_id
          itemId: $issue_id
          fieldId: $date_added_field_id
          value: $created_at_value
        }
      ) {
        projectNextItem {
          id
        }
      }
    }' -f project_id=$PLATFORM_SCRUM_PROJECT_ID -f issue_id=$1 -f date_added_field_id=$DATE_ADDED_FIELD_ID -f created_at_value=$2 > /dev/null
}

# for each issueId/createdAt element, call the update_issue function
for ISSUE_WITH_CREATED_AT in "${ISSUE_WITH_CREATED_AT_ARR[@]}"
do
  # jq concatenated issueId and createdAt with a '|', so now we split on '|'
  IFS='|' read ISSUE_ID CREATED_AT <<< "$ISSUE_WITH_CREATED_AT"
  update_issue $ISSUE_ID $CREATED_AT
done

echo "done"
