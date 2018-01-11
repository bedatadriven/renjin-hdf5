package org.renjin.hdf5.message;

import org.renjin.hdf5.HeaderReader;
import org.renjin.sexp.AttributeMap;

/**
 * The Attribute message is used to store objects in the HDF5 file which are used as attributes, or “metadata” about
 * the current object. An attribute is a small dataset; it has a name, a datatype, a dataspace, and raw data.
 *  Since attributes are stored in the object header, they should be relatively small (in other words, less than 64KB).
 *  They can be associated with any type of object which has an object header (groups, datasets, or
 *  committed (named) datatypes).
 *
 * <p>In 1.8.x versions of the library, attributes can be larger than 64KB. See the “Special Issues” section of the
 * Attributes chapter in the HDF5 User’s Guide for more information.
 *
 * <p>Note: Attributes on an object must have unique names: the HDF5 Library currently enforces this by causing the
 * creation of an attribute with a duplicate name to fail. Attributes on different objects may have the same name,
 * however.
 */
public class AttributeMessage extends Message {

  public static final int MESSAGE_TYPE = 0x0C;

  public AttributeMessage(HeaderReader reader) {

  }
}
