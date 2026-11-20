<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ display: string, datetime: string }>()
const parts = computed(() => /^(\S+) (\d{2}:\d{2}(?::\d{2})?)(.*)$/.exec(props.display))
</script>

<template>
  <time class="grid justify-items-start gap-0.5 leading-tight" :datetime="datetime" :title="display">
    <template v-if="parts">
      <span class="font-bold text-cp-text">{{ parts[2] }}</span>
      <span class="text-cp-xs text-cp-text-tertiary">{{ parts[1] }}{{ parts[3] }}</span>
    </template>
    <span v-else>{{ display }}</span>
  </time>
</template>
