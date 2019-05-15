package io.rsocket.frame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.util.NumberUtils;

import java.util.HashMap;
import java.util.Map;

class StreamMetadataFlyweight {


    private static final int STREAM_METADATA_KNOWN_MASK = 0x80; // 1000 0000
    private static final byte STREAM_METADATA_LENGTH_MASK = 0x7F; // 0111 1111

    private StreamMetadataFlyweight() {}

    /**
     * Decode the next mime type information from a composite metadata buffer which {@link ByteBuf#readerIndex()} is
     * at the start of the next metadata section.
     * <p>
     * Mime type is returned as a {@link String} containing only US_ASCII characters, and the index is moved past the
     * mime section, to the starting byte of the sub-metadata's length.
     *
     * @param buffer the metadata or composite metadata to read mime information from.
     * @return the next metadata mime type as {@link String}. the buffer {@link ByteBuf#readerIndex()} is moved.
     */
    static String decodeMimeFromMetadataHeader(ByteBuf buffer) {
        byte source = buffer.readByte();
        if ((source & STREAM_METADATA_KNOWN_MASK) == STREAM_METADATA_KNOWN_MASK) {
            //M flag set
            int id = source & STREAM_METADATA_LENGTH_MASK;
            WellKnownMimeType mime = WellKnownMimeType.fromId(id);
            if (mime != null) {
                return mime.toString();
            }
            else {
                throw new IllegalStateException(Integer.toBinaryString(source) + " is not an expected binary representation");
            }

        }
        //M flag unset, remaining 7 bits are the length of the mime
        int mimeLength = Byte.toUnsignedInt(source) + 1;
        CharSequence mime = buffer.readCharSequence(mimeLength, CharsetUtil.US_ASCII);
        return mime.toString();
    }

    /**
     * Decode the current metadata length information from a composite metadata buffer which {@link ByteBuf#readerIndex()}
     * is just past the current metadata section's mime information.
     * <p>
     * The index is moved past the metadata length section, to the starting byte of the current metadata's value.
     *
     * @param buffer the metadata or composite metadata to read length information from.
     * @return the next metadata length. the buffer {@link ByteBuf#readerIndex()} is moved.
     */
    static int decodeMetadataLengthFromMetadataHeader(ByteBuf buffer) {
        if (buffer.readableBytes() < 3) {
            throw new IllegalStateException("the given buffer should contain at least 3 readable bytes after decoding mime type");
        }
        return buffer.readUnsignedMedium();
    }

    /**
     * Encode a {@link WellKnownMimeType well known mime type} and a metadata value length into a newly allocated
     * {@link ByteBuf}.
     * <p>
     * This compact representation encodes the mime type via its ID on a single byte, and the unsigned value length on
     * 3 additional bytes.
     *
     * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
     * @param mimeType a {@link WellKnownMimeType} to encode.
     * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits integer.
     * @return the encoded mime and metadata length information
     */
    static ByteBuf encodeMetadataHeader(ByteBufAllocator allocator, WellKnownMimeType mimeType, int metadataLength) {
        ByteBuf buffer = allocator.buffer(4, 4)
                .writeByte(mimeType.getIdentifier() | STREAM_METADATA_KNOWN_MASK);

        NumberUtils.encodeUnsignedMedium(buffer, metadataLength);

        return buffer;
    }

    /**
     * Encode a custom mime type and a metadata value length into a newly allocated {@link ByteBuf}.
     * <p>
     * This larger representation encodes the mime type representation's length on a single byte, then the representation
     * itself, then the unsigned metadata value length on 3 additional bytes.
     *
     * @param allocator the {@link ByteBufAllocator} to use to create the buffer.
     * @param customMime a custom mime type to encode.
     * @param metadataLength the metadata length to append to the buffer as an unsigned 24 bits integer.
     * @return the encoded mime and metadata length information
     */
    static ByteBuf encodeMetadataHeader(ByteBufAllocator allocator, String customMime, int metadataLength) {
        ByteBuf mimeBuffer = allocator.buffer(customMime.length());
        mimeBuffer.writeCharSequence(customMime, CharsetUtil.UTF_8);
        if (!ByteBufUtil.isText(mimeBuffer, CharsetUtil.US_ASCII)) {
            throw new IllegalArgumentException("custom mime type must be US_ASCII characters only");
        }
        int ml = mimeBuffer.readableBytes();
        if (ml < 1 || ml > 128) {
            throw new IllegalArgumentException("custom mime type must have a strictly positive length that fits on 7 unsigned bits, ie 1-128");
        }

        ByteBuf mimeLength = allocator.buffer(1,1);
        mimeLength.writeByte(ml - 1);

        ByteBuf metadataLengthBuffer = allocator.buffer(3, 3);
        NumberUtils.encodeUnsignedMedium(metadataLengthBuffer, metadataLength);

        return allocator.compositeBuffer()
                .addComponents(true, mimeLength, mimeBuffer, metadataLengthBuffer);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param customMimeType the custom mime type to encode.
     * @param metadata the metadata value to encode.
     * @see #encodeMetadataHeader(ByteBufAllocator, String, int)
     */
    static void addMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, String customMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, customMimeType, metadata.readableBytes()),
                metadata);
    }

    /**
     * Encode a new sub-metadata information into a composite metadata {@link CompositeByteBuf buffer}.
     *
     * @param compositeMetaData the buffer that will hold all composite metadata information.
     * @param allocator the {@link ByteBufAllocator} to use to create intermediate buffers as needed.
     * @param knownMimeType the {@link WellKnownMimeType} to encode.
     * @param metadata the metadata value to encode.
     * @see #encodeMetadataHeader(ByteBufAllocator, WellKnownMimeType, int)
     */
    static void addMetadata(CompositeByteBuf compositeMetaData, ByteBufAllocator allocator, WellKnownMimeType knownMimeType, ByteBuf metadata) {
        compositeMetaData.addComponents(true,
                encodeMetadataHeader(allocator, knownMimeType, metadata.readableBytes()),
                metadata);
    }

    /**
     * Decode composite metadata information into a {@link Map} of {@link String} mime types to {@link ByteBuf} metadata
     * values.
     *
     * @param compositeMetadata the {@link ByteBuf} that contains information for one or more metadata mime-value pairs.
     * @param retainMetadataSlices should metadata value {@link ByteBuf} be {@link ByteBuf#retain() retained} when decoded?
     * @return the decoded composite metadata
     */
    public static Map<String, ByteBuf> decodeToMap(ByteBuf compositeMetadata, boolean retainMetadataSlices) {
        Map<String, ByteBuf> map = new HashMap<>();
        while (compositeMetadata.isReadable()) {
            String mime = decodeMimeFromMetadataHeader(compositeMetadata);
            int length = decodeMetadataLengthFromMetadataHeader(compositeMetadata);

            ByteBuf metadata = retainMetadataSlices ? compositeMetadata.readRetainedSlice(length) : compositeMetadata.readSlice(length);
            map.put(mime, metadata);
        }
        return map;
    }
}
