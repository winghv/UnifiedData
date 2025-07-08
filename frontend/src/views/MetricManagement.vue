<template>
  <div>
    <el-card>
      <template #header>
        <div class="card-header">
          <span>Metric Management</span>
          <el-button type="primary" @click="handleAdd">Add Metric</el-button>
        </div>
      </template>

      <el-table :data="metrics" stripe style="width: 100%">
        <el-table-column prop="name" label="Name" width="180"></el-table-column>
        <el-table-column prop="dataSourceType" label="Data Source Type" width="180"></el-table-column>
        <el-table-column prop="sourceUrl" label="Source URL" show-overflow-tooltip></el-table-column>
        <el-table-column label="Actions" width="180">
          <template #default="scope">
            <el-button size="small" @click="handleEdit(scope.row)">Edit</el-button>
            <el-button size="small" type="danger" @click="handleDelete(scope.row.id)">Delete</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <el-dialog v-model="dialogVisible" :title="isEdit ? 'Edit Metric' : 'Add Metric'" width="50%">
      <el-form :model="form" label-width="140px">
        <el-form-item label="Name">
          <el-input v-model="form.name"></el-input>
        </el-form-item>
        <el-form-item label="Data Source Type">
          <el-select v-model="form.dataSourceType" placeholder="Select type">
            <el-option label="HTTP JSON" value="HTTP_JSON"></el-option>
            <el-option label="HTTP CSV" value="HTTP_CSV"></el-option>
            <el-option label="File JSON" value="FILE_JSON"></el-option>
            <el-option label="File CSV" value="FILE_CSV"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Source URL">
          <el-input v-model="form.sourceUrl"></el-input>
        </el-form-item>
        <el-form-item label="Data Path (for JSON)">
          <el-input v-model="form.dataPath"></el-input>
        </el-form-item>
        <el-form-item label="Field Mappings">
          <pre>{{ form.fieldMappings }}</pre>
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
import { ref, onMounted } from 'vue';
import { ElMessage, ElMessageBox } from 'element-plus';
import metricService from '../services/metricService';

const metrics = ref([]);
const dialogVisible = ref(false);
const isEdit = ref(false);
const form = ref({
  id: null,
  name: '',
  dataSourceType: '',
  sourceUrl: '',
  dataPath: '',
  fieldMappings: {}
});

const fetchMetrics = async () => {
  try {
    const response = await metricService.getMetrics();
    metrics.value = response.data;
  } catch (error) {
    ElMessage.error('Failed to fetch metrics.');
  }
};

onMounted(fetchMetrics);

const handleAdd = () => {
  isEdit.value = false;
  form.value = {
    id: null,
    name: '',
    dataSourceType: 'HTTP_JSON',
    sourceUrl: '',
    dataPath: '',
    fieldMappings: { 'field_name': 'STRING' }
  };
  dialogVisible.value = true;
};

const handleEdit = (row) => {
  isEdit.value = true;
  form.value = { ...row };
  dialogVisible.value = true;
};

const handleDelete = async (id) => {
  try {
    await ElMessageBox.confirm('This will permanently delete the metric. Continue?', 'Warning', {
      confirmButtonText: 'OK',
      cancelButtonText: 'Cancel',
      type: 'warning',
    });
    await metricService.deleteMetric(id);
    ElMessage.success('Metric deleted successfully.');
    fetchMetrics();
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('Failed to delete metric.');
    }
  }
};

const handleSave = async () => {
  try {
    if (isEdit.value) {
      await metricService.updateMetric(form.value.id, form.value);
      ElMessage.success('Metric updated successfully.');
    } else {
      await metricService.createMetric(form.value);
      ElMessage.success('Metric added successfully.');
    }
    dialogVisible.value = false;
    fetchMetrics();
  } catch (error) {
    ElMessage.error('Failed to save metric.');
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
