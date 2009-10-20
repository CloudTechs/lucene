package org.apache.lucene.index.codecs;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.codecs.intblock.IntBlockCodec;
import org.apache.lucene.index.codecs.preflex.PreFlexCodec;
import org.apache.lucene.index.codecs.pulsing.PulsingCodec;
import org.apache.lucene.index.codecs.sep.SepCodec;
import org.apache.lucene.index.codecs.standard.StandardCodec;

/** Holds a set of codecs, keyed by name.  You subclass
 *  this, instantiate it, and register your codecs, then
 *  pass this instance to IndexReader/IndexWriter (via
 *  package private APIs) to use different codecs when
 *  reading & writing segments. */

public abstract class Codecs {

  private final HashMap codecs = new HashMap();

  private final Collection knownExtensions = new HashSet();

  public void register(Codec codec) {
    if (codec.name == null) {
      throw new IllegalArgumentException("code.name is null");
    }

    if (!codecs.containsKey(codec.name)) {
      codecs.put(codec.name, codec);
      codec.getExtensions(knownExtensions);
    } else if (codecs.get(codec.name) != codec) {
      throw new IllegalArgumentException("codec '" + codec.name + "' is already registered as a different codec instance");
    }
  }

  public Collection getAllExtensions() {
    return knownExtensions;
  }

  public Codec lookup(String name) {
    final Codec codec = (Codec) codecs.get(name);
    if (codec == null)
      throw new IllegalArgumentException("required codec '" + name + "' not found");
    return codec;
  }

  public abstract Codec getWriter(SegmentWriteState state);

  static private final Codecs defaultCodecs = new DefaultCodecs();

  public static Codecs getDefault() {
    return defaultCodecs;
  }
}

class DefaultCodecs extends Codecs {
  DefaultCodecs() {
    register(new StandardCodec());
    register(new IntBlockCodec());
    register(new PreFlexCodec());
    register(new PulsingCodec());
    register(new SepCodec());
  }

  public Codec getWriter(SegmentWriteState state) {
    return lookup("Standard");
    //return lookup("Pulsing");
    //return lookup("Sep");
    //return lookup("IntBlock");
  }
}