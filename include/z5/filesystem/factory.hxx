#pragma once

#include "z5/filesystem/metadata.hxx"
#include "z5/filesystem/dataset.hxx"

#include <array>

namespace z5 {
namespace filesystem {


    // factory function to open an existing dataset
    inline std::unique_ptr<z5::Dataset> openDataset(const handle::Dataset & dataset) {

        // make sure that the file exists
        if(!dataset.exists()) {
            throw std::runtime_error("Opening dataset failed because it does not exists.");
        }

        DatasetMetadata metadata;
        readMetadata(dataset, metadata);

        // make the ptr to the DatasetTyped of appropriate dtype
        std::unique_ptr<z5::Dataset> ptr;
        switch(metadata.dtype) {
            case types::int8:
                ptr.reset(new Dataset<int8_t>(dataset, metadata)); break;
            case types::int16:
                ptr.reset(new Dataset<int16_t>(dataset, metadata)); break;
            case types::int32:
                ptr.reset(new Dataset<int32_t>(dataset, metadata)); break;
            case types::int64:
                ptr.reset(new Dataset<int64_t>(dataset, metadata)); break;
            case types::uint8:
                ptr.reset(new Dataset<uint8_t>(dataset, metadata)); break;
            case types::uint16:
                ptr.reset(new Dataset<uint16_t>(dataset, metadata)); break;
            case types::uint32:
                ptr.reset(new Dataset<uint32_t>(dataset, metadata)); break;
            case types::uint64:
                ptr.reset(new Dataset<uint64_t>(dataset, metadata)); break;
            case types::float32:
                ptr.reset(new Dataset<float>(dataset, metadata)); break;
            case types::float64:
                ptr.reset(new Dataset<double>(dataset, metadata)); break;
            case types::complex64:
                ptr.reset(new Dataset<std::complex<float>>(dataset, metadata)); break;
            case types::complex128:
                ptr.reset(new Dataset<std::complex<double>>(dataset, metadata)); break;
            // TODO: extend metadata shape with unicode suffix length
            case types::unicode1:
                ptr.reset(new Dataset<z5::types::UTF32Array<1>>(dataset, metadata)); break;
            case types::unicode2:
                ptr.reset(new Dataset<z5::types::UTF32Array<2>>(dataset, metadata)); break;
            case types::unicode3:
                ptr.reset(new Dataset<z5::types::UTF32Array<3>>(dataset, metadata)); break;
            case types::unicode4:
                ptr.reset(new Dataset<z5::types::UTF32Array<4>>(dataset, metadata)); break;
            case types::unicode5:
                ptr.reset(new Dataset<z5::types::UTF32Array<5>>(dataset, metadata)); break;
            case types::unicode6:
                ptr.reset(new Dataset<z5::types::UTF32Array<6>>(dataset, metadata)); break;
            case types::unicode7:
                ptr.reset(new Dataset<z5::types::UTF32Array<7>>(dataset, metadata)); break;
            case types::unicode8:
                ptr.reset(new Dataset<z5::types::UTF32Array<8>>(dataset, metadata)); break;
            case types::unicode9:
                ptr.reset(new Dataset<z5::types::UTF32Array<9>>(dataset, metadata)); break;
            case types::unicode10:
                ptr.reset(new Dataset<z5::types::UTF32Array<10>>(dataset, metadata)); break;
        }
        return ptr;
    }


    // factory function to create a dataset
    inline std::unique_ptr<z5::Dataset> createDataset(
        const handle::Dataset & dataset,
        const DatasetMetadata & metadata
    ) {
        dataset.create();
        writeMetadata(dataset, metadata);

        // make the ptr to the DatasetTyped of appropriate dtype
        std::unique_ptr<z5::Dataset> ptr;
        switch(metadata.dtype) {
            case types::int8:
                ptr.reset(new Dataset<int8_t>(dataset, metadata)); break;
            case types::int16:
                ptr.reset(new Dataset<int16_t>(dataset, metadata)); break;
            case types::int32:
                ptr.reset(new Dataset<int32_t>(dataset, metadata)); break;
            case types::int64:
                ptr.reset(new Dataset<int64_t>(dataset, metadata)); break;
            case types::uint8:
                ptr.reset(new Dataset<uint8_t>(dataset, metadata)); break;
            case types::uint16:
                ptr.reset(new Dataset<uint16_t>(dataset, metadata)); break;
            case types::uint32:
                ptr.reset(new Dataset<uint32_t>(dataset, metadata)); break;
            case types::uint64:
                ptr.reset(new Dataset<uint64_t>(dataset, metadata)); break;
            case types::float32:
                ptr.reset(new Dataset<float>(dataset, metadata)); break;
            case types::float64:
                ptr.reset(new Dataset<double>(dataset, metadata)); break;
            case types::complex64:
                ptr.reset(new Dataset<std::complex<float>>(dataset, metadata)); break;
            case types::complex128:
                ptr.reset(new Dataset<std::complex<double>>(dataset, metadata)); break;
            // TODO: extend metadata shape with unicode suffix length
            case types::unicode1:
                ptr.reset(new Dataset<z5::types::UTF32Array<1>>(dataset, metadata)); break;
            case types::unicode2:
                ptr.reset(new Dataset<z5::types::UTF32Array<2>>(dataset, metadata)); break;
            case types::unicode3:
                ptr.reset(new Dataset<z5::types::UTF32Array<3>>(dataset, metadata)); break;
            case types::unicode4:
                ptr.reset(new Dataset<z5::types::UTF32Array<4>>(dataset, metadata)); break;
            case types::unicode5:
                ptr.reset(new Dataset<z5::types::UTF32Array<5>>(dataset, metadata)); break;
            case types::unicode6:
                ptr.reset(new Dataset<z5::types::UTF32Array<6>>(dataset, metadata)); break;
            case types::unicode7:
                ptr.reset(new Dataset<z5::types::UTF32Array<7>>(dataset, metadata)); break;
            case types::unicode8:
                ptr.reset(new Dataset<z5::types::UTF32Array<8>>(dataset, metadata)); break;
            case types::unicode9:
                ptr.reset(new Dataset<z5::types::UTF32Array<9>>(dataset, metadata)); break;
            case types::unicode10:
                ptr.reset(new Dataset<z5::types::UTF32Array<10>>(dataset, metadata)); break;
        }
        return ptr;
    }


    template<class GROUP>
    inline void createFile(const z5::handle::File<GROUP> & file, const bool isZarr) {
        file.create();
        Metadata fmeta(isZarr);
        writeMetadata(file, fmeta);
    }


    inline void createGroup(const handle::Group & group, const bool isZarr) {
        group.create();
        Metadata fmeta(isZarr);
        writeMetadata(group, fmeta);
    }


    template<class GROUP1, class GROUP2>
    inline std::string relativePath(const z5::handle::Group<GROUP1> & g1,
                                    const GROUP2 & g2) {
        return relativeImpl(g1.path(), g2.path()).string();
    }

}
}
