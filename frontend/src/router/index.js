import { createRouter, createWebHistory } from 'vue-router';
import MetricManagement from '../views/MetricManagement.vue';
import TableManagement from '../views/TableManagement.vue';
import SqlQuery from '../views/SqlQuery.vue';

const routes = [
  {
    path: '/',
    redirect: '/metrics'
  },
  {
    path: '/metrics',
    name: 'MetricManagement',
    component: MetricManagement
  },
  {
    path: '/tables',
    name: 'TableManagement',
    component: TableManagement
  },
  {
    path: '/query',
    name: 'SqlQuery',
    component: SqlQuery
  }
];

const router = createRouter({
  history: createWebHistory(),
  routes
});

export default router;
