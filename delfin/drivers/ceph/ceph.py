from delfin.drivers.ceph import rest_client
from delfin.drivers import driver


class CephStorageDriver(driver.StorageDriver):
    """CephStorageDriver implements Ceph Driver,
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = rest_client.RestClient(**kwargs)
       

    def reset_connection(self, context, **kwargs):
        pass

    def get_storage(self, context):
        #Clsuter Summary of Ceph

        storage = self.client.get_storage()

        s = {
            'name': 'OceanStor',
            'vendor': 'Huawei',
            'description': 'Huawei OceanStor Storage',
            'model': storage['NAME'],
            'status': status,
            'serial_number': storage['ID'],
            'firmware_version': firmware_ver,
            'location': storage['LOCATION'],
            'total_capacity': total_cap,
            'used_capacity': used_cap,
            'free_capacity': free_cap,
            'raw_capacity': raw_cap
        }
        LOG.info("get_storage(), successfully retrieved storage details")
        return s

    def list_storage_pools(self, context):
        try:
            # Get list of Ceph pool details
            pools = self.client.get_all_pools()

            pool_list = []
            for pool in pools:
                # Get pool status
                status = constants.StoragePoolStatus.OFFLINE
                if pool['RUNNINGSTATUS'] == consts.STATUS_POOL_ONLINE:
                    status = constants.StoragePoolStatus.NORMAL

                # Get pool storage_type
                storage_type = constants.StorageType.BLOCK
                if pool.get('USAGETYPE') == consts.FILE_SYSTEM_POOL_TYPE:
                    storage_type = constants.StorageType.FILE

                total_cap = \
                    int(pool['USERTOTALCAPACITY']) * self.sector_size
                used_cap = \
                    int(pool['USERCONSUMEDCAPACITY']) * self.sector_size
                free_cap = \
                    int(pool['USERFREECAPACITY']) * self.sector_size

                p = {
                    'name': pool['NAME'],
                    'storage_id': self.storage_id,
                    'native_storage_pool_id': pool['ID'],
                    'description': 'Huawei OceanStor Pool',
                    'status': status,
                    'storage_type': storage_type,
                    'total_capacity': total_cap,
                    'used_capacity': used_cap,
                    'free_capacity': free_cap,
                }
                pool_list.append(p)

            return pool_list

        except Exception:
            LOG.error("Failed to get pool metrics from OceanStor")
            raise