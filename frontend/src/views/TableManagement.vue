<template>
  <div>
    <el-card>
      <template #header>
        <div class="card-header">
          <span>Table Management</span>
          <el-button type="primary" @click="handleAdd">Add Table</el-button>
        </div>
      </template>

      <el-table :data="tables" stripe style="width: 100%">
        <el-table-column prop="tableName" label="Table Name" width="200"></el-table-column>
        <el-table-column prop="primaryKeys" label="Primary Keys" :formatter="row => row.primaryKeys.join(', ')"></el-table-column>
        <el-table-column prop="timeGranularity" label="Time Granularity" width="180"></el-table-column>
        <el-table-column label="Fields" width="600">
          <template #default="scope">
            <el-table :data="getFields(scope.row)" style="width: 100%" :show-header="false">
              <el-table-column prop="field" label="Field" width="200"></el-table-column>
              <el-table-column prop="metric" label="Metric" width="200"></el-table-column>
              <el-table-column prop="type" label="Type" width="200"></el-table-column>
            </el-table>
          </template>
        </el-table-column>
        <el-table-column label="Actions" width="180">
          <template #default="scope">
            <el-button size="small" @click="handleEdit(scope.row)">Edit</el-button>
            <el-button size="small" type="danger" @click="handleDelete(scope.row.id)">Delete</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="dialogVisible" :title="isEdit ? 'Edit Table' : 'Add Table'" width="60%">
      <el-form :model="form" label-width="140px">
        <el-form-item label="Table Name">
          <el-input v-model="form.tableName"></el-input>
        </el-form-item>
        <el-form-item label="Primary Keys">
          <el-input v-model="form.primaryKeys_str" placeholder="Comma-separated keys"></el-input>
        </el-form-item>
        <el-form-item label="Time Granularity">
          <el-select v-model="form.timeGranularity" placeholder="Select granularity">
            <el-option label="Minute" value="MINUTE"></el-option>
            <el-option label="Hour" value="HOUR"></el-option>
            <el-option label="Day" value="DAY"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Metric Field Mappings">
          <pre>{{ form.metricFields }}</pre>
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="dialogVisible = false">Cancel</el-button>
          <el-button type="primary" @click="handleSave">Save</el-button>
        </span>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import tableService from '../services/tableService';

const tables = ref([]);
const dialogVisible = ref(false);
const isEdit = ref(false);
const form = ref({
  id: null,
  tableName: '',
  primaryKeys: [],
  metricFields: {},
  timeGranularity: 'DAY'
});
// Helper for comma-separated string editing of primary keys
const primaryKeys_str = ref('');
const formatFieldMappings = (row) => {
  if (!row.fieldMapping) return '';
  return Object.entries(row.fieldMapping)
    .map(([logical, physical]) => `${logical} -> ${physical}`)
    .join(', ');
};

const formatMetricFields = (row) => {
  if (!row.metricFields) return '';
  return Object.entries(row.metricFields)
    .map(([field, metric]) => `${field} -> ${metric}`)
    .join(', ');
};

const formatFieldTypes = (row) => {
  if (!row.fieldTypes) return '';
  return Object.entries(row.fieldTypes)
    .map(([field, type]) => `${field}: ${type}`)
    .join(', ');
};

const getFields = (row) => {
  const fields = [];
  if (!row.metricFields || !row.fieldTypes) return fields;
  
  // Get unique fields
  const uniqueFields = new Set(Object.keys(row.metricFields));
  
  uniqueFields.forEach(field => {
    const metrics = row.metricFields[field];
    const type = row.fieldTypes[field] || 'N/A';
    
    // If multiple metrics for a field, join them with comma
    const metricDisplay = Array.isArray(metrics) ? metrics.join(', ') : metrics;
    
    fields.push({
      field,
      metric: metricDisplay,
      type
    });
  });
  
  return fields;
};

watch(() => form.value.primaryKeys, (newVal) => {
  primaryKeys_str.value = newVal.join(', ');
});

const fetchTables = async () => {
  try {
    const response = await tableService.getTables();
    tables.value = response.data;
  } catch (error) {
    ElMessage.error('Failed to fetch tables.');
  }
};

onMounted(fetchTables);

const handleAdd = () => {
  isEdit.value = false;
  form.value = {
    id: null,
    tableName: '',
    primaryKeys: [],
    metricFields: { 'field_name': 'metric_name' },
    timeGranularity: 'DAY'
  };
  primaryKeys_str.value = '';
  dialogVisible.value = true;
};

const handleEdit = (row) => {
  isEdit.value = true;
  form.value = { ...row };
  dialogVisible.value = true;
};

const handleDelete = async (id) => {
  try {
    await ElMessageBox.confirm('This will permanently delete the table. Continue?', 'Warning', {
      confirmButtonText: 'OK',
      cancelButtonText: 'Cancel',
      type: 'warning',
    });
    await tableService.deleteTable(id);
    ElMessage.success('Table deleted successfully.');
    fetchTables();
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('Failed to delete table.');
    }
  }
};

const handleSave = async () => {
  // Update primaryKeys array from the string input
  form.value.primaryKeys = primaryKeys_str.value.split(',').map(k => k.trim()).filter(k => k);

  try {
    if (isEdit.value) {
      await tableService.updateTable(form.value.id, form.value);
      ElMessage.success('Table updated successfully.');
    } else {
      await tableService.createTable(form.value);
      ElMessage.success('Table added successfully.');
    }
    dialogVisible.value = false;
    fetchTables();
  } catch (error) {
    ElMessage.error('Failed to save table.');
  }
};
</script>

<style scoped>
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
