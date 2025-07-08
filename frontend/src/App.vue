<template>
  <el-container class="app-container">
    <el-aside width="220px" class="app-aside">
      <div class="logo-container">
        <h3>Data Platform</h3>
      </div>
      <el-menu
        :default-active="$route.path"
        class="el-menu-vertical-demo"
        router
      >
        <el-menu-item index="/metrics">
          <span>Metric Management</span>
        </el-menu-item>
        <el-menu-item index="/tables">
          <span>Table Management</span>
        </el-menu-item>
        <el-menu-item index="/query">
          <span>SQL Query</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-main class="app-main">
      <router-view></router-view>
    </el-main>
  </el-container>
</template>

<script setup>
// No script needed for this basic layout
</script>

<style>
html, body, #app, .app-container {
  height: 100%;
  margin: 0;
  padding: 0;
  font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
}

.app-aside {
  background-color: #f4f4f5;
  border-right: 1px solid #e6e6e6;
}

.logo-container {
  padding: 20px;
  text-align: center;
  background-color: #409EFF;
  color: white;
}

.el-menu-vertical-demo:not(.el-menu--collapse) {
  width: 220px;
  border-right: none;
}

.app-main {
  padding: 20px;
}
</style>
