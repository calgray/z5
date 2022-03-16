import unittest
import os
from shutil import rmtree
from abc import ABC

import numpy as np
import z5py


class DatasetTestMixin(ABC):
    def setUp(self):
        self.shape = (100, 100, 100)
        self.path = 'array.' + self.data_format
        self.root_file = z5py.File(self.path, use_zarr_format=self.data_format == 'zarr')

        self.base_dtypes = [
            'int8', 'int16', 'int32', 'int64',
            'uint8', 'uint16', 'uint32', 'uint64',
            'float32', 'float64'
        ]
        self.dtypes = tuple(
            self.base_dtypes +
            [np.dtype(s) for s in self.base_dtypes] +
            [
                '<i1', '<i2', '<i4', '<i8',
                '<u1', '<u2', '<u4', '<u8',
                '<f4', '<f8'
            ] +
            [
                np.int8, np.int16, np.int32, np.int64,
                np.uint8, np.uint16, np.uint32, np.uint64,
                np.float32, np.float64
            ]
        )

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def check_array(self, result, expected, msg=None):
        self.assertEqual(result.shape, expected.shape, msg)
        self.assertTrue(np.allclose(result, expected), msg)

    def test_ds_open_empty(self):
        self.root_file.create_dataset('test',
                                      dtype='float32',
                                      shape=self.shape,
                                      chunks=(10, 10, 10))
        ds = self.root_file['test']
        out = ds[:]
        self.check_array(out, np.zeros(self.shape))

    def test_ds_dtypes(self):
        shape = (100, 100)
        chunks = (10, 10)
        for dtype in self.dtypes:
            ds = self.root_file.create_dataset('data_%s' % hash(dtype),
                                               dtype=dtype,
                                               shape=shape,
                                               chunks=chunks)
            in_array = np.random.rand(*shape).astype(dtype)
            ds[:] = in_array
            out_array = ds[:]
            self.check_array(out_array, in_array,
                             'datatype %s failed for format %s' % (self.data_format.title(),
                                                                   dtype))

    def check_ones(self, sliced_ones, expected_shape, msg=None):
        self.check_array(sliced_ones, np.ones(expected_shape, dtype=np.uint8), msg)

    def test_ds_simple_write(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = np.ones(self.shape, np.uint8)

    def test_ds_indexing(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = np.ones(self.shape, np.uint8)

        self.check_ones(ds[:], self.shape, 'full index failed')

        self.check_ones(ds[1, ...], (100, 100), 'trailing ellipsis failed')
        self.check_ones(ds[..., 1], (100, 100), 'leading ellipsis failed')
        self.check_ones(ds[1], (100, 100), 'implicit ellipsis failed')
        self.check_ones(ds[:, :, :, ...], self.shape, 'superfluous ellipsis failed')
        self.check_ones(ds[500:501, :, :], (0, 100, 100), 'out-of-bounds slice failed')
        self.check_ones(ds[-501:500, :, :], (0, 100, 100), 'negative out-of-bounds slice failed')

        self.check_ones(ds[1, :, :], (100, 100), 'integer index failed')
        self.check_ones(ds[-20:, :, :], (20, 100, 100), 'negative slice failed')

        self.assertEqual(ds[1, 1, 1], 1, 'point index failed')

        with self.assertRaises(ValueError):
            ds[500, :, :]
        with self.assertRaises(ValueError):
            ds[-500, :, :]
        with self.assertRaises(ValueError):
            ds[..., :, ...]
        with self.assertRaises(ValueError):
            ds[1, 1, slice(0, 100, 2)]
        with self.assertRaises(TypeError):
            ds[[1, 1, 1]]  # explicitly test behaviour different to h5py

        class NotAnIndex(object):
            pass

        with self.assertRaises(TypeError):
            ds[1, 1, NotAnIndex()]

    def test_ds_scalar_broadcast(self):
        for dtype in self.base_dtypes:
            ds = self.root_file.create_dataset('ones_%s' % dtype,
                                               dtype=dtype,
                                               shape=self.shape,
                                               chunks=(10, 10, 10))
            ds[:] = 1
            self.check_ones(ds[:], self.shape)

    def test_ds_scalar_broadcast_from_float(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = float(1)
        self.check_ones(ds[:], self.shape),

    def test_ds_scalar_broadcast_from_bool(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = True
        self.check_ones(ds[:], self.shape)

    def test_ds_set_with_arraylike(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[0, :2, :2] = [[1, 1], [1, 1]]
        self.check_ones(ds[0, :2, :2], (2, 2))

    def test_ds_set_from_float(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = np.ones(self.shape, dtype=float)
        self.check_ones(ds[:], self.shape)

    def test_ds_set_from_bool(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        ds[:] = np.ones(self.shape, dtype=bool)
        self.check_ones(ds[:], self.shape)

    def test_ds_fancy_broadcast_fails(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        with self.assertRaises(ValueError):
            ds[0, :10, :10] = np.ones(10, dtype=np.uint8)

    def test_ds_write_object_fails(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))

        class ArbitraryObject(object):
            pass

        with self.assertRaises(OSError):
            ds[0, 0, :2] = [ArbitraryObject(), ArbitraryObject()]

    def test_ds_write_flexible_fails(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8,
                                           shape=self.shape, chunks=(10, 10, 10))
        with self.assertRaises(TypeError):
            ds[0, 0, 0] = "hey, you're not a number"

    def test_readwrite_multithreaded(self):
        for n_threads in (1, 2, 4, 8):
            ds = self.root_file.create_dataset('data_mthread_%i' % n_threads,
                                               dtype='float64',
                                               shape=self.shape,
                                               chunks=(10, 10, 10),
                                               n_threads=n_threads)
            in_array = np.random.rand(*self.shape)
            ds[:] = in_array
            out_array = ds[:]
            self.check_array(out_array, in_array)

    def test_create_nested_dataset(self):
        self.root_file.create_dataset('group/sub_group/data',
                                      shape=self.shape,
                                      dtype='float64',
                                      chunks=(10, 10, 10))
        self.assertTrue(os.path.exists(os.path.join(self.path, 'group', 'sub_group', 'data')))

    def test_create_with_data(self):
        in_array = np.random.rand(*self.shape)
        ds = self.root_file.create_dataset('data', data=in_array)
        out_array = ds[:]
        self.check_array(out_array, in_array)

    def test_require_dataset(self):
        in_array = np.random.rand(*self.shape)
        self.root_file.require_dataset('data', data=in_array,
                                       shape=in_array.shape,
                                       dtype=in_array.dtype)
        ds = self.root_file.require_dataset('data',
                                            shape=in_array.shape,
                                            dtype=in_array.dtype)
        out_array = ds[:]
        self.check_array(out_array, in_array)

    def test_non_contiguous(self):
        ds = self.root_file.create_dataset('test',
                                           dtype='float32',
                                           shape=self.shape,
                                           chunks=(10, 10, 10))
        # make a non-contiguous 3d array of the correct shape (100)^3
        vol = np.arange(200**3).astype('float32').reshape((200, 200, 200))
        in_array = vol[::2, ::2, ::2]
        ds[:] = in_array
        out_array = ds[:]
        self.check_array(out_array, in_array, 'failed for non-contiguous data')

    def test_empty_chunk(self):
        ds = self.root_file.create_dataset('test',
                                           dtype='float32',
                                           shape=self.shape,
                                           chunks=(10, 10, 10))
        bb = np.s_[:10, :10, :10]
        if ds.is_zarr:
            chunk_path = os.path.join(self.path, 'test', '0.0.0')
        else:
            chunk_path = os.path.join(self.path, 'test', '0', '0', '0')
        ds[bb] = 0
        self.assertFalse(os.path.exists(chunk_path))
        ds[bb] = 1
        self.assertTrue(os.path.exists(chunk_path))
        ds[bb] = 0
        self.assertFalse(os.path.exists(chunk_path))

    def test_invalid_options(self):
        with self.assertRaises(RuntimeError):
            self.root_file.create_dataset('test1', shape=self.shape, dtype='float32',
                                          chunks=(10, 10, 10), compression='raw',
                                          level=5)
        with self.assertRaises(RuntimeError):
            self.root_file.create_dataset('test2', shape=self.shape, dtype='float32',
                                          chunks=(10, 10, 10), compression='gzip',
                                          level=5, blub='blob')

    def test_readwrite_chunk(self):
        shape = (100, 100)
        chunks = (10, 10)
        for dtype in self.base_dtypes:
            ds = self.root_file.create_dataset('test_%s' % dtype, dtype=dtype,
                                               shape=shape, chunks=chunks,
                                               compression='raw')
            # test empty chunk
            out = ds.read_chunk((0, 0))
            self.assertEqual(out, None)

            # test read/write
            chunks_per_dim = ds.chunks_per_dimension
            for x in range(chunks_per_dim[0]):
                for y in range(chunks_per_dim[1]):
                    data = np.random.rand(*chunks)
                    if dtype not in ('float32', 'float64'):
                        data *= 128
                    data = data.astype(dtype)
                    ds.write_chunk((x, y), data)
                    out = ds.read_chunk((x, y))
                    self.assertEqual(data.shape, out.shape)
                    self.assertTrue(np.allclose(data, out))

    def test_read_direct(self):
        shape = (100, 100)
        chunks = (10, 10)

        ds = self.root_file.create_dataset('test', dtype='float64',
                                           shape=shape, chunks=chunks,
                                           compression='raw')

        # generate test data
        data = np.random.rand(*shape)
        ds[:] = data

        # test reading full dataset
        out = np.zeros(shape)
        ds.read_direct(out)
        self.assertTrue(np.allclose(out, data))

        # test reading with selection
        selection = np.s_[11:53, 67:84]
        out = np.zeros(shape)
        ds.read_direct(out, selection, selection)
        self.assertTrue(np.allclose(out[selection], data[selection]))

    def test_write_direct(self):
        shape = (100, 100)
        chunks = (10, 10)

        ds = self.root_file.create_dataset('test', dtype='float64',
                                           shape=shape, chunks=chunks,
                                           compression='raw')

        # generate test data
        data = np.random.rand(*shape)

        # test writing full dataset
        ds.write_direct(data)
        out = ds[:]
        self.assertTrue(np.allclose(out, data))

        # test writing with selection
        ds[:] = 0
        selection = np.s_[11:53, 67:84]
        ds.write_direct(data, selection, selection)
        out = ds[:]
        self.assertTrue(np.allclose(out[selection], data[selection]))

    def test_irregular_chunks(self):
        shape = (123, 54, 211)
        chunks = (13, 33, 22)

        ds = self.root_file.create_dataset('test', dtype='float64',
                                           shape=shape, chunks=chunks,
                                           compression='raw')
        data = np.random.rand(*shape)
        ds[:] = data
        out = ds[:]
        self.assertTrue(np.allclose(out, data))

    def test_nd(self):
        f = self.root_file
        for ndim in range(1, 6):
            size = 100 if ndim < 4 else 20
            shape = (size,) * ndim
            chunks = (10,) * ndim
            ds = f.create_dataset('test_%i' % ndim, dtype='float64',
                                  shape=shape, chunks=chunks, compression='raw')

            data = np.random.rand(*shape)
            ds[:] = data
            out = ds[:]
            self.assertTrue(np.allclose(out, data))

    def test_no_implicit_squeeze(self):
        arr = np.ones((5, 5, 5))
        ds = self.root_file.create_dataset('ds', data=arr)

        self.assertEqual(ds[:, 0:1, :].shape, arr[:, 0:1, :].shape)

    def test_no_implicit_squeeze_singleton(self):
        """Issue #102

        https://github.com/constantinpape/z5/issues/102
        """
        arr = np.ones((5, 5, 5))
        ds = self.root_file.create_dataset('ds', data=arr)
        self.assertEqual(
            ds[0:1, 0:1, 0:1].shape,
            arr[0:1, 0:1, 0:1].shape,
        )

    def test_explicit_squeeze(self):
        """Issue #103

        https://github.com/constantinpape/z5/issues/103
        """
        arr = np.full((5, 4, 3), 1)
        ds = self.root_file.create_dataset('ds543', data=arr)
        self.assertEqual(ds[:, 1, :].shape, arr[:, 1, :].shape)

        self.assertNotIsInstance(ds[1, 1, 1], np.ndarray)

    def test_singleton_dtype(self):
        """Issue #102

        https://github.com/constantinpape/z5/issues/102
        """
        arr = np.ones((5, 5, 5))
        ds = self.root_file.create_dataset('ds', data=arr)
        self.assertEqual(type(ds[1, 1, 1]), type(arr[1, 1, 1]))

    def test_broadcast_empty(self):
        """Issue #107

        https://github.com/constantinpape/z5/issues/107
        """
        ds = self.root_file.create_dataset('test', shape=(100, 100), chunks=(25, 25),
                                           dtype='uint8', compression='raw')
        ds[:20, :20] = 1
        out = ds[:]
        self.assertTrue(np.allclose(out[:20, :20], 1))

    def test_empty_chunks_non_aligned_write(self):
        """Issue #106

        https://github.com/constantinpape/z5/issues/106
        """
        ds = self.root_file.create_dataset(name='test', shape=(128,), chunks=(32,),
                                           compression='raw', dtype='uint8')

        inp = np.ones((100,), dtype='uint8')
        inp[90:100] = 0
        ds[:100] = inp
        # last chunk should be empty, but this is not the case if buffer was not
        # cleared correctly
        out = ds[-32:]
        self.assertTrue(np.allclose(out, 0))

    def test_fill_value(self):
        """Check fill values, esp. special values for zarr, see Issue #148

        https://github.com/constantinpape/z5/issues/148
        """
        shape = (100, 100)
        chunks = (10, 10)
        for i, fillval in enumerate(self.fill_values):
            ds = self.root_file.create_dataset('fval_%i' % i, dtype='float32',
                                               shape=shape, chunks=chunks,
                                               fillvalue=fillval)
            data = ds[:]
            exp = np.full(shape, fillval, dtype='float32')
            self.assertTrue(np.allclose(data, exp, equal_nan=np.isnan(fillval)))

    def test_parent(self):
        f = self.root_file
        shape = (100, 100)
        chunks = (10, 10)
        ds = f.create_dataset('test', dtype='float32', shape=shape, chunks=chunks)
        self.assertIs(f, ds.parent)

    def test_name(self):
        f = self.root_file
        shape = (100, 100)
        chunks = (10, 10)
        ds = f.create_dataset('test', dtype='float32', shape=shape, chunks=chunks)
        self.assertEqual(ds.name, '/test')
        g = f.create_group('g')
        ds = g.create_dataset('test', dtype='float32', shape=shape, chunks=chunks)
        self.assertEqual(ds.name, '/g/test')

    def test_file(self):
        f = self.root_file
        shape = (100, 100)
        chunks = (10, 10)
        ds = f.create_dataset('test', dtype='float32', shape=shape, chunks=chunks)
        self.assertIs(ds.file, f)


class TestZarrDataset(DatasetTestMixin, unittest.TestCase):
    data_format = 'zarr'
    fill_values = [0, 42, np.nan, np.inf, -np.inf]

    def test_varlen(self):
        shape = (100, 100)
        chunks = (10, 10)
        ds = self.root_file.create_dataset('varlen', dtype='float64',
                                           shape=shape, chunks=chunks,
                                           compression='raw')
        with self.assertRaises(RuntimeError):
            ds.write_chunk((0, 0), np.random.rand(10), True)

    def test_nested(self):
        f = z5py.ZarrFile(self.path, mode='w', dimension_separator='/')
        data = np.random.rand(*self.shape)
        chunks = (10, 10, 10)
        ds = f.create_dataset('data', data=data, chunks=chunks)
        self.assertTrue(os.path.exists(
            os.path.join(self.path, 'data/0/0/0')
        ))

        res = ds[:]
        self.assertTrue(np.allclose(data, res))

        g = z5py.File(self.path)
        res = g['data'][:]
        self.assertTrue(np.allclose(data, res))

    def test_nested2(self):
        f = z5py.ZarrFile(self.path, mode='w', dimension_separator='/')
        chunks = (10, 10, 10)
        f.create_dataset('data', shape=self.shape, chunks=chunks, dtype='float64')

        g = z5py.ZarrFile(self.path, mode='a')
        ds = f['data']
        data = np.random.rand(*self.shape)
        ds[:] = data
        self.assertTrue(os.path.exists(
            os.path.join(self.path, 'data/0/0/0')
        ))

        res = ds[:]
        self.assertTrue(np.allclose(data, res))


class TestN5Dataset(DatasetTestMixin, unittest.TestCase):
    data_format = 'n5'
    fill_values = [0]

    def test_varlen(self):
        # 5 * 5 =  25 chunks
        shape = (50, 50)
        chunks = (10, 10)

        def _test_data(max_len, dtype):
            dlen = np.random.randint(1, max_len)
            if dtype.startswith('float'):
                return np.random.rand(dlen).astype(dtype)
            elif dtype.startswith('uint'):
                return np.random.randint(0, 255, size=(dlen,), dtype=dtype)
            else:
                return np.random.randint(-126, 126, size=(dlen,), dtype=dtype)

        def _test_vlen(dtype, compression):
            name = 'vlen_%s_%s' % (dtype, compression)
            ds = self.root_file.create_dataset(name, dtype=dtype,
                                               shape=shape, chunks=chunks,
                                               compression=compression)

            max_len = 1023
            chunks_per_dim = ds.chunks_per_dimension
            for x in range(chunks_per_dim[0]):
                for y in range(chunks_per_dim[1]):
                    test_data = _test_data(max_len, dtype)
                    ds.write_chunk((x, y), test_data, True)
                    out = ds.read_chunk((x, y))
                    self.assertEqual(test_data.shape, out.shape)
                    self.assertTrue(np.allclose(test_data, out))

        # parameters for testing:
        # 2 integer dtypes, 2 unsigned dtypes and the 2 float dtypes
        dtypes = ['int8', 'int32', 'uint16', 'uint64', 'float32', 'float64']
        # raw and gzip compression
        compressions = ['raw', 'gzip']

        for dtype in dtypes:
            for compression in compressions:
                _test_vlen(dtype, compression)


if __name__ == '__main__':
    unittest.main()
