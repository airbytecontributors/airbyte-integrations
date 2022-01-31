#!/usr/bin/env sh

my_org="airbytehq"
project_id="PN_kwDOA4_XW84AAj7t"

gh api graphql -f query='
  query($project_id: ID!) {
    node(id: $project_id) {
      ... on ProjectNext {
        items(first: 100) {
          nodes(ids: ["PNI_lADOA4_XW84AAj7tzgAZ3-Y"]) {
            id
          }
        }
      }
    }
  }' -f project_id=$project_id
