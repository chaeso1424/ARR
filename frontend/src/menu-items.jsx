const menuItems = {
  items: [
    {
      id: 'navigation',
      title: 'Navigation',
      type: 'group',
      icon: 'icon-navigation',
      children: [
        {
          id: 'dashboard',
          title: 'Dashboard',
          type: 'item',
          icon: 'feather icon-home',
          url: '/app/dashboard/analytics',
          breadcrumbs: false
        },
        {
          id: 'logs',
          title: 'Logs',
          type: 'item',
          icon: 'feather icon-home',
          url: '/app/Logs',
          breadcrumbs: false
        }
      ]
    },
  ]
};

export default menuItems;
