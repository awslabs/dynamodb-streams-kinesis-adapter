#!/bin/bash

KCL_MAVEN_DIR=~/.m2/repository/software/amazon/kinesis/amazon-kinesis-client

KCL_PREVIOUS_VERSION=""
KCL_PREVIOUS_JAR=""
KCL_CURRENT_VERSION=""
KCL_CURRENT_JAR=""

# KCL concrete classes the adapter extends and overrides methods on.
KCL_PARENT_CLASSES=(
  "software.amazon.kinesis.retrieval.polling.PollingConfig"
  "software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory"
  "software.amazon.kinesis.leases.HierarchicalShardSyncer"
  "software.amazon.kinesis.lifecycle.KinesisConsumerTaskFactory"
  "software.amazon.kinesis.leases.LeaseCleanupManager"
)

get_kcl_jars() {
  KCL_CURRENT_VERSION=$(mvn -q -Dexec.executable=echo \
    -Dexec.args='${software-kinesis.version}' --non-recursive exec:exec)
  echo "Current KCL version (from pom.xml): $KCL_CURRENT_VERSION"

  local base_ref="${GITHUB_BASE_REF:-master}"
  local remote="origin"
  git fetch "${remote}" "${base_ref}" --depth=1 2>/dev/null

  KCL_PREVIOUS_VERSION=$(git show "${remote}/${base_ref}:pom.xml" 2>/dev/null \
    | grep -A1 '<software-kinesis.version>' \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' \
    | head -1)

  if [[ -z "$KCL_PREVIOUS_VERSION" ]]; then
    echo "Could not determine previous KCL version from base branch. Skipping."
    exit 0
  fi

  echo "Previous KCL version (from ${base_ref}): $KCL_PREVIOUS_VERSION"

  if [[ "$KCL_PREVIOUS_VERSION" == "$KCL_CURRENT_VERSION" ]]; then
    echo "KCL version unchanged. No upstream changes to check."
    exit 0
  fi

  mvn -B dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-client:$KCL_CURRENT_VERSION
  KCL_CURRENT_JAR=$KCL_MAVEN_DIR/$KCL_CURRENT_VERSION/amazon-kinesis-client-$KCL_CURRENT_VERSION.jar

  mvn -B dependency:get -Dartifact=software.amazon.kinesis:amazon-kinesis-client:$KCL_PREVIOUS_VERSION
  KCL_PREVIOUS_JAR=$KCL_MAVEN_DIR/$KCL_PREVIOUS_VERSION/amazon-kinesis-client-$KCL_PREVIOUS_VERSION.jar
}

# Normalize javap output for stable comparison: strip synchronized, sort method lines.
normalize_javap() {
  sed 's/synchronized //g' | grep -E '^\s' | sort
}

check_kcl_class() {
  local class="$1"

  local previous_output=$(javap -classpath "$KCL_PREVIOUS_JAR" "$class" 2>/dev/null)
  local current_output=$(javap -classpath "$KCL_CURRENT_JAR" "$class" 2>/dev/null)

  if [[ -z "$previous_output" ]]; then
    return
  fi

  if [[ -z "$current_output" ]]; then
    echo "  WARNING - Class $class no longer exists in KCL $KCL_CURRENT_VERSION"
    echo "::warning::Class removed in KCL $KCL_CURRENT_VERSION: $class"
    return
  fi

  local previous_normalized=$(echo "$previous_output" | normalize_javap)
  local current_normalized=$(echo "$current_output" | normalize_javap)

  local removed=$(diff <(echo "$previous_normalized") <(echo "$current_normalized") | grep '^<' | sed 's/^< //')
  local added=$(diff <(echo "$previous_normalized") <(echo "$current_normalized") | grep '^>' | sed 's/^> //')

  if [[ -n "$removed" ]]; then
    echo "  WARNING - Methods removed or signatures changed in $class:"
    echo "$removed" | while IFS= read -r line; do
      echo "    - $line"
      echo "::warning::Method removed/changed in $class: $line"
    done
  fi

  if [[ -n "$added" ]]; then
    local new_abstract=$(echo "$added" | grep 'abstract ')
    local new_methods=$(echo "$added" | grep -v 'abstract ')

    if [[ -n "$new_abstract" ]]; then
      echo "  WARNING - New abstract methods added to $class (adapter must implement):"
      echo "$new_abstract" | while IFS= read -r line; do
        echo "    - $line"
        echo "::warning::New abstract method in $class: $line"
      done
    fi

    if [[ -n "$new_methods" ]]; then
      echo "  WARNING - New methods/overloads added to $class (may affect adapter overrides):"
      echo "$new_methods" | while IFS= read -r line; do
        echo "    - $line"
        echo "::warning::New method/overload in $class: $line"
      done
    fi
  fi
}

main() {
  echo "=== KCL Upstream Compatibility Check ==="
  get_kcl_jars
  echo "Comparing KCL $KCL_PREVIOUS_VERSION (base branch) vs $KCL_CURRENT_VERSION (current)"

  for class in "${KCL_PARENT_CLASSES[@]}"; do
    check_kcl_class "$class"
  done

  echo "Done."
}

main
