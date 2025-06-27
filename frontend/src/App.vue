<template>
  <el-container class="app-container">
    <el-header class="app-header">
      <h1>Unified Data Service</h1>
    </el-header>
    <el-main>
      <el-tabs v-model="activeTab" type="card">
        <el-tab-pane label="Configuration Management" name="config">
          <el-card>
            <div class="toolbar">
              <el-button type="primary" @click="handleAdd">Add Configuration</el-button>
            </div>
        <el-table :data="configurations" stripe style="width: 100%">
          <el-table-column prop="name" label="Name" width="180"></el-table-column>
          <el-table-column prop="configType" label="Type" width="150"></el-table-column>
          <el-table-column prop="configValue" label="Value" show-overflow-tooltip>
             <template #default="scope">
                <pre class="value-pre">{{ formatValue(scope.row.configValue) }}</pre>
            </template>
          </el-table-column>
          <el-table-column prop="description" label="Description" show-overflow-tooltip></el-table-column>
          <el-table-column label="Actions" width="240">
            <template #default="scope">
              <el-button size="small" @click="handleEdit(scope.row)">Edit</el-button>
              <el-button size="small" type="danger" @click="handleDelete(scope.row.id)">Delete</el-button>
              <el-button size="small" type="info" @click="handlePreview(scope.row)">Preview</el-button>
            </template>
          </el-table-column>
        </el-table>
          </el-card>
        </el-tab-pane>
        
        <el-tab-pane label="Stock Market Data" name="stocks">
          <stock-data-viewer />
        </el-tab-pane>
      </el-tabs>
    </el-main>

    <el-dialog v-model="dialogVisible" :title="isEdit ? 'Edit Configuration' : 'Add Configuration'" width="50%">
      <el-form :model="form" label-width="120px">
        <el-form-item label="Name">
          <el-input v-model="form.name"></el-input>
        </el-form-item>
        <el-form-item label="Type">
          <el-input v-model="form.configType"></el-input>
        </el-form-item>
        <el-form-item label="Value">
          <el-input v-model="form.configValue" type="textarea" :rows="5"></el-input>
        </el-form-item>
        <el-form-item label="Description">
          <el-input v-model="form.description"></el-input>
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="cancelForm">Cancel</el-button>
          <el-button type="primary" @click="saveConfiguration">Save</el-button>
        </span>
      </template>
    </el-dialog>
  </el-container>
</template>

<script setup>
import { ref, onMounted } from 'vue';
import axios from 'axios';
import { ElMessage, ElMessageBox } from 'element-plus';
import StockDataViewer from './components/StockDataViewer.vue';

const configurations = ref([]);
const activeTab = ref('config');
const dialogVisible = ref(false);
const isEdit = ref(false);
const form = ref({
  id: '',
  name: '',
  configType: '',
  configValue: '',
  description: ''
});

const API_URL = 'http://localhost:8080/api/configurations';

const fetchConfigurations = async () => {
  try {
    const response = await axios.get(API_URL);
    configurations.value = response.data;
  } catch (error) {
    ElMessage.error('Error fetching configurations.');
    console.error(error);
  }
};

onMounted(() => {
  fetchConfigurations();
});

const resetForm = () => {
  form.value = {
    id: null,
    name: '',
    configType: '',
    configValue: '',
    description: ''
  };
  isEdit.value = false;
};

const handleAdd = () => {
  resetForm();
  dialogVisible.value = true;
};

const handleEdit = (row) => {
  form.value = { ...row };
  isEdit.value = true;
  dialogVisible.value = true;
};

const handleDelete = (id) => {
  ElMessageBox.confirm('This will permanently delete the configuration. Continue?', 'Warning', {
    confirmButtonText: 'OK',
    cancelButtonText: 'Cancel',
    type: 'warning',
  }).then(async () => {
    try {
      await axios.delete(`${API_URL}/${id}`);
      ElMessage.success('Delete completed');
      fetchConfigurations();
    } catch (error) {
      ElMessage.error('Error deleting configuration.');
      console.error(error);
    }
  }).catch(() => {
    ElMessage.info('Delete canceled');
  });
};

const handlePreview = (row) => {
    ElMessageBox.alert(`<pre>${formatValue(row.configValue)}</pre>`, `Preview: ${row.name}`, {
        dangerouslyUseHTMLString: true,
    });
};

const cancelForm = () => {
  dialogVisible.value = false;
  resetForm();
};

const saveConfiguration = async () => {
  try {
    if (isEdit.value) {
      await axios.put(`${API_URL}/${form.value.id}`, form.value);
      ElMessage.success('Configuration updated successfully');
    } else {
      await axios.post(API_URL, form.value);
      ElMessage.success('Configuration added successfully');
    }
    fetchConfigurations();
    cancelForm();
  } catch (error) {
    ElMessage.error('Error saving configuration.');
    console.error(error);
  }
};

const formatValue = (value) => {
    try {
        const parsed = JSON.parse(value);
        return JSON.stringify(parsed, null, 2);
    } catch (e) {
        return value;
    }
};

</script>

<style>
.app-container {
  padding: 20px;
}
.app-header {
  text-align: center;
}
.toolbar {
  margin-bottom: 20px;
  text-align: right;
}
.value-pre {
    white-space: pre-wrap;
    word-break: break-all;
}
</style>
