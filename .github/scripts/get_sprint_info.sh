#!/usr/bin/env sh

my_org="airbytehq"
project_id="PN_kwDOA4_XW84AAj7t"

gh api graphql -f query='
  query($project_id: ID!) {
    node(id: $project_id) {
      ... on ProjectNext {
        fields(first: 100) {
          nodes {
            id
            name
            settings
          }
        }
      }
    }
  }' -f project_id=$project_id | jq '.data.node.fields.nodes[] | select( .name == "Sprint") | .settings | fromjson.configuration.iterations[] | select(.title | contains ("5"))'
