package arrow

// \brief Compress the buffer to spaced, excluding the null entries.
//
// \param[in] src the source buffer
// \param[in] num_values the size of source buffer
// \param[in] valid_bits bitmap data indicating position of valid slots
// \param[in] valid_bits_offset offset into valid_bits
// \param[out] output the output buffer spaced
// \return The size of spaced buffer.
func SpacedCompress(
	src interface{}, numValues int, validBits uint8,
	validBitsOffset int64, output *interface{}) int {
	panic("not yet implemented")
}
