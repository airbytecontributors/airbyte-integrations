#set -e

VERSION=$(cat .env | grep "^VERSION=" | cut -d = -f 2)

error() {
  echo "$@"
  exit 1
}

assert_root() {
  [ -f .root ] || error "Must run from root"
}

_get_fullpath() {
  echo $(cd $1 && pwd)
}

_docker_get_label() {
  local dockerfile=$1
  local label=$2
  local label_value; label_value=$(< "$dockerfile" grep $2 | cut -d = -f2)

  [ "$label_value" == "" ] && error "Missing label $label in $dockerfile"
  echo $label_value
}

_docker_get_version() {
  _docker_get_label $1 io.airbyte.version
}

_docker_get_name() {
  _docker_get_label $1 io.airbyte.name
}

_docker_get_latest_image() {
  echo "$(_docker_get_name $1):latest"
}

_docker_get_current_image() {
  echo "$(_docker_get_name $1):$(_docker_get_version $1)"
}

_docker_check_tag_exists() {
  local image=$1

  DOCKER_CLI_EXPERIMENTAL=enabled docker manifest inspect "$image" > /dev/null
}

_docker_build() {
  local dockerfile=$1
  local context; context=$(dirname $dockerfile)
  local image_name; image_name=$(_docker_get_name $dockerfile)

  docker build -f $dockerfile -t $image_name:dev $context
}

_docker_publish() {
  local dockerfile=$1
  local image_name; image_name=$(_docker_get_name $dockerfile)
  local image_version; image_version=$(_docker_get_version $dockerfile)

  local dev_image="$image_name:dev"
  local latest_image="$image_name:latest"
  local versioned_image="$image_name:$image_version"

  docker tag $dev_image $latest_image
  docker tag $dev_image $versioned_image

  if [ -z "$FORCE_PUBLISH" ] && _docker_check_tag_exists $versioned_image; then
    error "You're trying to push an version that was already released ($versioned_image). Make sure you bump it up."
  fi

  echo "Publishing new version ($versioned_image)"
  docker push $versioned_image
  docker push $latest_image
}
