package org.renjin.hdf5.message;


public class Filter {

    public static final int FILTER_DEFLATE = 1;
    public static final int FILTER_SHUFFLE = 2;
    public static final int FILTER_FLETCHER32 = 3;
    public static final int FILTER_SZIP = 4;
    public static final int FILTER_NBIT = 5;
    public static final int FILTER_SCALE_OFFSET = 6;

    private int filterId;
    private final String name;
    private final int[] clientData;
    private final boolean optional;

    Filter(int filterId, String name, int[] clientData, boolean optional) {
        this.filterId = filterId;
        this.name = name;
        this.clientData = clientData;
        this.optional = optional;
    }

    /**
     * Unique identifier for the filter.
     *
     * <p>Values from zero through 32,767 are reserved for filters supported by The HDF Group in the HDF5 Library
     * and for filters requested and supported by third parties. Filters supported by The HDF Group are documented
     * immediately below. Information on 3rd-party filters can be found at The HDF Group’s Contributions page.
     *
     * <p>Values from 32768 to 65535 are reserved for non-distributed uses (for example, internal company usage) or
     * for application usage when testing a feature. The HDF Group does not track or document the use of the filters
     * with identifiers from this range.</p>
     */
    public int getFilterId() {
        return filterId;
    }

    /**
     *
     * @return the optional name of the filter
     */
    public String getName() {
        return name;
    }

    /**
     * Each filter can store integer values to control how the filter operates.
     */
    public int[] getClientData() {
        return clientData;
    }

    /**
     * If set then the filter is an optional filter. During output, if an optional filter fails it will
     * be silently skipped in the pipeline.
     */
    public boolean isOptional() {
        return optional;
    }
}
