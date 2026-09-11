<script setup lang="ts">
import type { AccountGroupFormValue } from '../composables/useAccountGroups'
import type { AccountGroup } from '@/api'
import { computed } from 'vue'

import BaseButton from '@/components/base/BaseButton.vue'
import BaseColorPicker from '@/components/base/BaseColorPicker/index.vue'
import BaseFormItem from '@/components/base/BaseForm/FormItem.vue'
import BaseForm from '@/components/base/BaseForm/index.vue'
import BaseInput from '@/components/base/BaseInput.vue'
import BaseModal from '@/components/base/BaseModal/index.vue'
import BaseTextarea from '@/components/base/BaseTextarea.vue'
import { ACCOUNT_GROUP_COLOR_PRESETS } from '../constants'

const props = defineProps<{
  group: AccountGroup | null
  saving: boolean
}>()
const emit = defineEmits<{
  save: []
}>()
const open = defineModel<boolean>({ required: true })
const form = defineModel<AccountGroupFormValue>('form', { required: true })
const title = computed(() => props.group ? '编辑分组' : '创建分组')
const description = computed(() => props.group
  ? '修改分组名称和用途说明。'
  : '创建后，可在账号管理中将账号加入这个分组。')
</script>

<template>
  <BaseModal
    v-model="open"
    :title="title"
    :description="description"
    size="md"
    :dismissible="!saving"
  >
    <BaseForm class="grid gap-5">
      <BaseFormItem label="分组名称" required>
        <BaseInput
          v-model="form.name"
          aria-label="分组名称"
          placeholder="例如：生产账号"
          :disabled="saving"
        />
      </BaseFormItem>
      <BaseFormItem label="分组颜色" required>
        <BaseColorPicker
          v-model="form.color"
          label="选择分组颜色"
          :presets="ACCOUNT_GROUP_COLOR_PRESETS"
          :disabled="saving"
        />
      </BaseFormItem>
      <div class="grid gap-4 sm:grid-cols-2">
        <BaseFormItem label="每用户日限额（USD）">
          <BaseInput v-model="form.dailyLimitUsd" type="number" min="0" step="any" aria-label="每用户日限额" :disabled="saving" />
        </BaseFormItem>
        <BaseFormItem label="每用户周限额（USD）">
          <BaseInput v-model="form.weeklyLimitUsd" type="number" min="0" step="any" aria-label="每用户周限额" :disabled="saving" />
        </BaseFormItem>
      </div>
      <p class="m-0 text-cp-sm text-cp-text-secondary">
        0 表示不限。每位用户在本组的所有密钥共享额度；用户之间独立计算。
      </p>
      <div class="grid gap-3" role="group" aria-label="模型计费倍率">
        <div class="flex items-center justify-between gap-3">
          <span class="text-cp-sm font-semibold">模型计费倍率</span>
          <BaseButton size="sm" :disabled="saving || form.modelMultipliers.length >= 200" @click="form.modelMultipliers.push({ model: '', multiplier: '1' })">
            添加模型
          </BaseButton>
        </div>
        <p class="m-0 text-cp-sm text-cp-text-secondary">
          按请求模型名称精确匹配，未配置时为 1 倍。原始费用 × 倍率计入消费及日/周限额，历史请求不变。
        </p>
        <div v-for="(row, index) in form.modelMultipliers" :key="index" class="grid grid-cols-[minmax(0,1fr)_6rem_auto] items-center gap-2">
          <BaseInput v-model="row.model" :aria-label="`模型名称 ${index + 1}`" placeholder="例如 gpt-5.5" :disabled="saving" maxlength="256" />
          <BaseInput v-model="row.multiplier" :aria-label="`计费倍率 ${index + 1}`" type="number" min="0" max="1000" step="any" :disabled="saving" />
          <BaseButton size="sm" :disabled="saving" @click="form.modelMultipliers.splice(index, 1)">
            移除
          </BaseButton>
        </div>
      </div>
      <BaseFormItem label="描述（可选）">
        <BaseTextarea
          v-model="form.description"
          aria-label="分组描述"
          :rows="4"
          placeholder="说明这个分组的用途..."
          :disabled="saving"
        />
      </BaseFormItem>
    </BaseForm>

    <template #footer>
      <BaseButton variant="secondary" :disabled="saving" @click="open = false">
        取消
      </BaseButton>
      <BaseButton
        variant="primary"
        :loading="saving"
        :disabled="!form.name.trim()"
        @click="emit('save')"
      >
        保存分组
      </BaseButton>
    </template>
  </BaseModal>
</template>
