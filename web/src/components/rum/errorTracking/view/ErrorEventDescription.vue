<!-- Copyright 2023 Zinc Labs Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http:www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License. 
-->

<template>
  <div class="description">
    <template
      v-if="
        column.type === 'view' && column.view_loading_type === 'route_change'
      "
    >
      <pre class="navigation q-pa-sm">
{
  <span class="text-primary">from</span> : {{ column.view_referrer }},
  <span class="text-primary">to</span> : {{ column.view_url }}
}</pre>
    </template>
    <template
      v-else-if="column.type === 'resource' && column.resource_type === 'xhr'"
    >
      <span class="text-bold q-pr-sm" style="font-size: 12px">{{
        column.resource_method
      }}</span>
      <a
        :href="column.resource_url"
        target="_blank"
        class="resource-url text-primary"
        >{{ column.resource_url }}</a
      >
      <span class="q-pl-sm">[ {{ column.resource_status_code }} ]</span>
    </template>
    <template v-else>
      <span style="font-size: 14px">{{ getDescription }}</span>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, defineProps } from "vue";

const props = defineProps({
  column: {
    type: Object,
    required: true,
  },
});

// resource : resource_url
// error : error_message
// view : view_referrer -> view_ur
// action :  _oo_action_target_text : _oo_action_target_selector
const getDescription = computed(() => {
  if (props.column["type"] === "resource") {
    return props.column["resource_url"];
  } else if (props.column["type"] === "error") {
    return props.column["error_message"];
  } else if (props.column["type"] === "view") {
    if (props.column.view_loading_type === "route_change") {
      return props.column["view_referrer"] + " to " + props.column["view_url"];
    }
    return props.column["view_url"];
  } else if (props.column["type"] === "action") {
    return (
      props.column["_oo_action_target_text"] +
      " : " +
      props.column["_oo_action_target_selector"]
    );
  }
  return "";
});
</script>

<style scoped>
.description {
  word-wrap: break-word;
  overflow: hidden;
  white-space: break-spaces;
}

.navigation {
  background-color: #ececec;
  border-radius: 4px;
}

.resource-url {
  text-decoration: none;
  font-size: 14px;
}
</style>
