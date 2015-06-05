# -*- coding: utf-8 -*-
"""
    Utility functions to handle ZIO devices.
"""

import select
import struct


CTRLBLOCK_SIZE = 512

CTRL_BLOCK_FIELDS = (
    'major_version', 'minor_version', 'zio_alarms', 'dev_alarms', 'seq_number', 'nsamples', 'ssize', 'nbits',
    'fam', 'type', 'host_id', 'dev_id',
    'cset', 'chan', ('dev_name', lambda f: f.rstrip('\0')),
    'tstamp_secs', 'tstamp_ticks',
    'tstamp_bins', 'mem_addr', 'reserved',
    'flags', ('trig_name', lambda f: f.rstrip('\0'))
)

CTRL_STRUCT_MASK = '=BBBBIIHH' + 'HHQI' + 'HH12s' + 'QQ' + 'QII' + 'I12s'


def get_timestamp(ctrl_blk):
    """
    :param ctrl_blk: the control block with the timestamp
    :return: a tuple with the seconds and the nanoseconds of the block
    """
    return ctrl_blk['tstamp_secs'], ctrl_blk['tstamp_ticks']


def get_channel(ctrl_blk):
    """
    :param ctrl_blk: the control block with the channel
    :return: the channel of the ctrl_block
    """
    return ctrl_blk['chan']


def _dump_ctrl_block_attrs(raw):
    """
    Return std and ext attributes and mask from the raw bytes
    :param raw: attributes bytes readed from the ZIO ctrl device
    :return: dict with the standard and the extended attributes (v1.0)
    """
    std_mask, _, ext_mask  = struct.unpack('HHI', raw[:8])
    std_attrs = struct.unpack('I' * 16, raw[8:72])
    ext_attrs = struct.unpack('I' * 32, raw[72:200])
    return {
        'std_mask': std_mask,
        'ext_mask': ext_mask,
        'std_attrs': std_attrs,
        'ext_attrs': ext_attrs
    }


def _dump_ctrl_block(raw):
    """
    Read the control block data
    :param raw: the stream read from the ctrl device
    :return: a dict with all the ctrl data
    """
    blk = {}
    attrs = zip(CTRL_BLOCK_FIELDS, struct.unpack(CTRL_STRUCT_MASK, raw[:96]))
    # if there is a function to handle the raw data, call it, otherwise use raw data
    for k, v in attrs:
        if type(k) in (tuple, list):
            key, filter = k
            blk[key] = filter(v)
        else:
            blk[k] = v
    return blk


def read_ctrl_block(dev_fd):
    """
    :param dev_fd: opened ZIO-ctrl file descriptor
    :return:dict with the ZIO control fields (v1.0)
    """
    ATTR_CHANNEL_START = 96
    ATTR_LEN = 200
    ATTR_TRIGGER_START = ATTR_CHANNEL_START + ATTR_LEN
    raw = dev_fd.read(CTRLBLOCK_SIZE)
    if len(raw) < CTRLBLOCK_SIZE:
        raise Exception('Read only %d bytes instead of %d' % (len(raw), CTRLBLOCK_SIZE))
    # create the dict with the ZIO ctrl fields
    blk = _dump_ctrl_block(raw)
    # add std and ext attributes for channel and trigger
    blk['attr_channel'] = _dump_ctrl_block_attrs(raw[ATTR_CHANNEL_START:])
    blk['attr_trigger'] = _dump_ctrl_block_attrs(raw[ATTR_TRIGGER_START:])
    return blk


def _dump_data(raw, nsamples, ssize=1):
    # data format
    if ssize == 2:
        fmt = 'H'
    elif ssize == 4:
        fmt = 'I'
    elif ssize == 8:
        fmt = 'Q'
    else:
        fmt = 'B'
    return struct.unpack('<' + fmt * nsamples, raw)


def read_data_block(dev_fd, nsamples, ssize=1):
    """
    :param dev_fd: opened ZIO-data file descriptor
    :param nsamples: number of samples to read
    :param ssize: size (in bytes) of samples
    :return: list with data read from ZIO-data device
    """
    tot_bytes = nsamples * ssize
    raw = dev_fd.read(tot_bytes)
    if len(raw) is not tot_bytes:
        raise
    return _dump_data(raw, nsamples, ssize)


def read_channel(ctrl_dev, data_dev):
    """
    Read data from the ZIO channel
    :param ctrl_dev: opened ZIO-ctrl file descriptor
    :param data_dev: opened ZIO-data file descriptor
    :return: interpreted ctrl_block and data_block
    """
    ctrl_blk = read_ctrl_block(ctrl_dev)
    data_blk = read_data_block(data_dev, ctrl_blk['nsamples'], ctrl_blk['ssize'])
    return ctrl_blk, data_blk


def enum_devices(base_device, channels):
    """
    Return a list of ZIO-channels valid for the open_devices method.
    :param base_device: path for the device and the cset, without the channel and type part (e.g. /dev/zio/zzero-0000-0)
    :param channels: could be a list of int or a single integer. If it is a list, open all the channels in the list, otherwise open all the first channels-th channels
    :return: a list of devices valid for the open_devices method
    """
    devs = []
    if type(channels) == int:
        channels = range(channels)
    base_device += '-%d' if len(channels) < 10 else '-%02d'
    for c in channels:
        b = base_device % int(c)
        devs.append('%s-ctrl' % b)
        devs.append('%s-data' % b)
    return devs


def open_devices(args):
    """
    Open ZIO devices and returns a dict of type {'ctrl_fd': 'data_fd'}
    :param args: list of ZIO-ctrl, ZIO-data (e.g ["dev0-ctrl", "dev0-data", "dev1-ctrl", "dev1-data"])
    :return:dictionary of opened file descriptors (e.g. {'ctrl_fd0': 'data_fd0', 'ctrl_fd1': 'data_fd1'})
    """
    ziodevs = {}
    try:
        for i in range(0, len(args), 2):
            cfd = open(args[i])
            dfd = open(args[i + 1])
            ziodevs[cfd] = dfd
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
        raise
    return ziodevs


def read_data(zio_devices, nblocks=-1):
    """
    :param nblocks: the number of blocks to read. NOTE: with multiple channels is not guaranteed to read exactly nblocks blocks of data, because it depends on select
    :param zio_devices: list of ZIO-ctrl, ZIO-data (e.g ["dev0-ctrl", "dev0-data", "dev1-ctrl", "dev1-data"])
    :return: generator with the first ctrl_block and data_block available from the list of zio_devices
    """
    ziodevs = open_devices(zio_devices)
    ctrl_devs = ziodevs.keys()
    while nblocks != 0:
        if nblocks > 0:
            nblocks -= 1
        readable, _, _ = select.select(ctrl_devs, [], [])
        for ctrldev in readable:
            datadev= ziodevs[ctrldev]
            yield read_channel(ctrldev, datadev)
