package org.renjin.hdf5.groups;

import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5Data;
import org.renjin.hdf5.message.LinkMessage;
import org.renjin.repackaged.guava.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleGroupIndex implements GroupIndex {

  private Hdf5Data file;
  private List<LinkMessage> links = new ArrayList<>();

  public SimpleGroupIndex(Hdf5Data file, Iterable<LinkMessage> messages) {
    this.file = file;
    Iterables.addAll(links, messages);
  }

  @Override
  public DataObject getObject(String name) throws IOException {
    for (LinkMessage link : links) {
      if (link.getLinkName().equals(name)) {
        return file.objectAt(link.getAddress());
      }
    }
    throw new IllegalArgumentException("No such link: " + name);
  }
}
