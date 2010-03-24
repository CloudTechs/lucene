package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.CodecProvider;
import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PostingsConsumer;
import org.apache.lucene.index.codecs.TermsConsumer;
import org.apache.lucene.index.codecs.sep.SepCodec;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.MultiCodecTestCase;
import org.apache.lucene.util.Version;

// TODO: test multiple codecs here?

// TODO
//   - test across fields
//   - fix this test to run once for all codecs
//   - make more docs per term, to test > 1 level skipping
//   - test all combinations of payloads/not and omitTF/not
//   - test w/ different indexDivisor
//   - test field where payload length rarely changes
//   - 0-term fields
//   - seek/skip to same term/doc i'm already on
//   - mix in deleted docs
//   - seek, skip beyond end -- assert returns false
//   - seek, skip to things that don't exist -- ensure it
//     goes to 1 before next one known to exist
//   - skipTo(term)
//   - skipTo(doc)

public class TestCodecs extends MultiCodecTestCase {

  private Random RANDOM;
  private static String[] fieldNames = new String[] {"one", "two", "three", "four"};

  private final static int NUM_TEST_ITER = 4000;
  private final static int NUM_TEST_THREADS = 3;
  private final static int NUM_FIELDS = 4;
  private final static int NUM_TERMS_RAND = 50; // must be > 16 to test skipping
  private final static int DOC_FREQ_RAND = 500; // must be > 16 to test skipping
  private final static int TERM_DOC_FREQ_RAND = 20;

  // start is inclusive and end is exclusive
  public int nextInt(final int start, final int end) {
    return start + RANDOM.nextInt(end-start);
  }

  private int nextInt(final int lim) {
    return RANDOM.nextInt(lim);
  }

  char[] getRandomText() {

    final int len = 1+this.nextInt(10);
    final char[] buffer = new char[len+1];
    for(int i=0;i<len;i++) {
      buffer[i] = (char) this.nextInt(97, 123);
      /*
      final int t = nextInt(5);
      if (0 == t && i < len-1) {
        // Make a surrogate pair
        // High surrogate
        buffer[i++] = (char) nextInt(0xd800, 0xdc00);
        // Low surrogate
        buffer[i] = (char) nextInt(0xdc00, 0xe000);
      } else if (t <= 1)
        buffer[i] = (char) nextInt(0x80);
      else if (2 == t)
        buffer[i] = (char) nextInt(0x80, 0x800);
      else if (3 == t)
        buffer[i] = (char) nextInt(0x800, 0xd800);
      else
        buffer[i] = (char) nextInt(0xe000, 0xffff);
    */
    }
    buffer[len] = 0xffff;
    return buffer;
  }

  class FieldData implements Comparable {
    final FieldInfo fieldInfo;
    final TermData[] terms;
    final boolean omitTF;
    final boolean storePayloads;

    public FieldData(final String name, final FieldInfos fieldInfos, final TermData[] terms, final boolean omitTF, final boolean storePayloads) {
      this.omitTF = omitTF;
      this.storePayloads = storePayloads;
      fieldInfos.add(name, true);
      fieldInfo = fieldInfos.fieldInfo(name);
      fieldInfo.omitTermFreqAndPositions = omitTF;
      fieldInfo.storePayloads = storePayloads;
      this.terms = terms;
      for(int i=0;i<terms.length;i++)
        terms[i].field = this;

      Arrays.sort(terms);
    }

    public int compareTo(final Object other) {
      return fieldInfo.name.compareTo(((FieldData) other).fieldInfo.name);
    }

    public void write(final FieldsConsumer consumer) throws Throwable {
      if (Codec.DEBUG)
        System.out.println("WRITE field=" + fieldInfo.name);
      Arrays.sort(terms);
      final TermsConsumer termsConsumer = consumer.addField(fieldInfo);
      for (final TermData term : terms)
        term.write(termsConsumer);
      termsConsumer.finish();
    }
  }

  class PositionData {
    int pos;
    BytesRef payload;

    PositionData(final int pos, final BytesRef payload) {
      this.pos = pos;
      this.payload = payload;
    }
  }

  class TermData implements Comparable {
    String text2;
    final BytesRef text;
    int[] docs;
    PositionData[][] positions;
    FieldData field;

    public TermData(final String text, final int[] docs, final PositionData[][] positions) {
      this.text = new BytesRef(text);
      this.text2 = text;
      this.docs = docs;
      this.positions = positions;
    }

    public int compareTo(final Object o) {
      return text2.compareTo(((TermData) o).text2);
    }

    public void write(final TermsConsumer termsConsumer) throws Throwable {
      if (Codec.DEBUG)
        System.out.println("  term=" + text2);
      final PostingsConsumer postingsConsumer = termsConsumer.startTerm(text);
      for(int i=0;i<docs.length;i++) {
        final int termDocFreq;
        if (field.omitTF) {
          termDocFreq = 0;
        } else {
          termDocFreq = positions[i].length;
        }
        postingsConsumer.startDoc(docs[i], termDocFreq);
        if (!field.omitTF) {
          for(int j=0;j<positions[i].length;j++) {
            final PositionData pos = positions[i][j];
            postingsConsumer.addPosition(pos.pos, pos.payload);
          }
          postingsConsumer.finishDoc();
        }
      }
      termsConsumer.finishTerm(text, docs.length);
    }
  }

  final private static String SEGMENT = "0";

  TermData[] makeRandomTerms(final boolean omitTF, final boolean storePayloads) {
    final int numTerms = 1+this.nextInt(NUM_TERMS_RAND);
    //final int numTerms = 2;
    final TermData[] terms = new TermData[numTerms];

    final HashSet<String> termsSeen = new HashSet<String>();

    for(int i=0;i<numTerms;i++) {

      // Make term text
      char[] text;
      String text2;
      while(true) {
        text = this.getRandomText();
        text2 = new String(text, 0, text.length-1);
        if (!termsSeen.contains(text2)) {
          termsSeen.add(text2);
          break;
        }
      }

      final int docFreq = 1+this.nextInt(DOC_FREQ_RAND);
      final int[] docs = new int[docFreq];
      PositionData[][] positions;

      if (!omitTF)
        positions = new PositionData[docFreq][];
      else
        positions = null;

      int docID = 0;
      for(int j=0;j<docFreq;j++) {
        docID += this.nextInt(1, 10);
        docs[j] = docID;

        if (!omitTF) {
          final int termFreq = 1+this.nextInt(TERM_DOC_FREQ_RAND);
          positions[j] = new PositionData[termFreq];
          int position = 0;
          for(int k=0;k<termFreq;k++) {
            position += this.nextInt(1, 10);

            final BytesRef payload;
            if (storePayloads && this.nextInt(4) == 0) {
              final byte[] bytes = new byte[1+this.nextInt(5)];
              for(int l=0;l<bytes.length;l++) {
                bytes[l] = (byte) this.nextInt(255);
              }
              payload = new BytesRef(bytes);
            } else {
              payload = null;
            }

            positions[j][k] = new PositionData(position, payload);
          }
        }
      }

      terms[i] = new TermData(text2, docs, positions);
    }

    return terms;
  }

  public void testFixedPostings() throws Throwable {

    RANDOM = this.newRandom();

    final int NUM_TERMS = 100;
    final TermData[] terms = new TermData[NUM_TERMS];
    for(int i=0;i<NUM_TERMS;i++) {
      final int[] docs = new int[] {1};
      final String text = Integer.toString(i, Character.MAX_RADIX);
      terms[i] = new TermData(text, docs, null);
    }

    final FieldInfos fieldInfos = new FieldInfos();

    final FieldData field = new FieldData("field", fieldInfos, terms, true, false);
    final FieldData[] fields = new FieldData[] {field};

    final Directory dir = new MockRAMDirectory();
    this.write(fieldInfos, dir, fields);
    final SegmentInfo si = new SegmentInfo(SEGMENT, 10000, dir, CodecProvider.getDefault().getWriter(null));
    si.setHasProx(false);

    final FieldsProducer reader = si.getCodec().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, 64, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR));

    final FieldsEnum fieldsEnum = reader.iterator();
    assertNotNull(fieldsEnum.next());
    final TermsEnum termsEnum = fieldsEnum.terms();
    for(int i=0;i<NUM_TERMS;i++) {
      final BytesRef term = termsEnum.next();
      assertNotNull(term);
      assertEquals(terms[i].text2, term.utf8ToString());
    }
    assertNull(termsEnum.next());

    for(int i=0;i<NUM_TERMS;i++) {
      assertEquals(termsEnum.seek(new BytesRef(terms[i].text2)), TermsEnum.SeekStatus.FOUND);
    }

    assertNull(fieldsEnum.next());
  }

  public void testRandomPostings() throws Throwable {

    RANDOM = this.newRandom();

    final FieldInfos fieldInfos = new FieldInfos();

    final FieldData[] fields = new FieldData[NUM_FIELDS];
    for(int i=0;i<NUM_FIELDS;i++) {
      final boolean omitTF = 0==(i%3);
      final boolean storePayloads = 1==(i%3);
      fields[i] = new FieldData(fieldNames[i], fieldInfos, this.makeRandomTerms(omitTF, storePayloads), omitTF, storePayloads);
    }

    final Directory dir = new MockRAMDirectory();

    this.write(fieldInfos, dir, fields);
    final SegmentInfo si = new SegmentInfo(SEGMENT, 10000, dir, CodecProvider.getDefault().getWriter(null));

    if (Codec.DEBUG) {
      System.out.println("\nTEST: now read");
    }

    final FieldsProducer terms = si.getCodec().fieldsProducer(new SegmentReadState(dir, si, fieldInfos, 1024, IndexReader.DEFAULT_TERMS_INDEX_DIVISOR));

    final Verify[] threads = new Verify[NUM_TEST_THREADS-1];
    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i] = new Verify(fields, terms);
      threads[i].setDaemon(true);
      threads[i].start();
    }

    new Verify(fields, terms).run();

    for(int i=0;i<NUM_TEST_THREADS-1;i++) {
      threads[i].join();
      assert !threads[i].failed;
    }

    terms.close();
    dir.close();
  }

  public void testSepPositionAfterMerge() throws IOException {
    final Directory dir = new RAMDirectory();
    final IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_31,
      new WhitespaceAnalyzer(Version.LUCENE_31));
    config.setCodecProvider(new SepCodecs());
    final IndexWriter writer = new IndexWriter(dir, config);

    try {
      final PhraseQuery pq = new PhraseQuery();
      pq.add(new Term("content", "bbb"));
      pq.add(new Term("content", "ccc"));

      final Document doc = new Document();
      doc.add(new Field("content", "aaa bbb ccc ddd", Store.NO, Field.Index.ANALYZED_NO_NORMS));

      // add document and force commit for creating a first segment
      writer.addDocument(doc);
      writer.commit();

      ScoreDoc[] results = this.search(writer, pq, 5);
      assertEquals(1, results.length);
      assertEquals(0, results[0].doc);

      // add document and force commit for creating a second segment
      writer.addDocument(doc);
      writer.commit();

      // at this point, there should be at least two segments
      results = this.search(writer, pq, 5);
      assertEquals(2, results.length);
      assertEquals(0, results[0].doc);

      writer.optimize();

      // optimise to merge the segments.
      results = this.search(writer, pq, 5);
      assertEquals(2, results.length);
      assertEquals(0, results[0].doc);
    }
    finally {
      writer.close();
      dir.close();
    }
  }

  private ScoreDoc[] search(final IndexWriter writer, final Query q, final int n) throws IOException {
    final IndexReader reader = writer.getReader();
    final IndexSearcher searcher = new IndexSearcher(reader);
    try {
      return searcher.search(q, null, n).scoreDocs;
    }
    finally {
      searcher.close();
      reader.close();
    }
  }

  public static class SepCodecs extends CodecProvider {

    protected SepCodecs() {
      this.register(new SepCodec());
    }

    @Override
    public Codec getWriter(final SegmentWriteState state) {
      return this.lookup("Sep");
    }

  }

  private String getDesc(final FieldData field, final TermData term) {
    return field.fieldInfo.name + ":" + term.text2;
  }

  private String getDesc(final FieldData field, final TermData term, final int doc) {
    return this.getDesc(field, term) + ":" + doc;
  }

  private class Verify extends Thread {
    final Fields termsDict;
    final FieldData[] fields;
    volatile boolean failed;

    Verify(final FieldData[] fields, final Fields termsDict) {
      this.fields = fields;
      this.termsDict = termsDict;
    }

    @Override
    public void run() {
      try {
        this._run();
      } catch (final Throwable t) {
        failed = true;
        throw new RuntimeException(t);
      }
    }

    private void verifyDocs(final int[] docs, final PositionData[][] positions, final DocsEnum docsEnum, final boolean doPos) throws Throwable {
      for(int i=0;i<docs.length;i++) {
        final int doc = docsEnum.nextDoc();
        assertTrue(doc != DocIdSetIterator.NO_MORE_DOCS);
        assertEquals(docs[i], doc);
        if (doPos) {
          this.verifyPositions(positions[i], ((DocsAndPositionsEnum) docsEnum));
        }
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsEnum.nextDoc());
    }

    byte[] data = new byte[10];

    private void verifyPositions(final PositionData[] positions, final DocsAndPositionsEnum posEnum) throws Throwable {
      for(int i=0;i<positions.length;i++) {
        final int pos = posEnum.nextPosition();
        if (Codec.DEBUG) {
          System.out.println("TEST pos " + (1+i) + " of " + positions.length + " pos=" + pos);
        }
        assertEquals(positions[i].pos, pos);
        if (positions[i].payload != null) {
          assertTrue(posEnum.hasPayload());
          if (TestCodecs.this.nextInt(3) < 2) {
            // Verify the payload bytes
            final BytesRef otherPayload = posEnum.getPayload();
            if (Codec.DEBUG) {
              System.out.println("TEST do check payload len=" + posEnum.getPayloadLength() + " vs " + (otherPayload == null ? "null" : otherPayload.length));
            }

            assertTrue("expected=" + positions[i].payload.toString() + " got=" + otherPayload.toString(), positions[i].payload.equals(otherPayload));
          } else {
            if (Codec.DEBUG) {
              System.out.println("TEST skip check payload len=" + posEnum.getPayloadLength());
            }
          }
        } else {
          assertFalse(posEnum.hasPayload());
        }
      }
    }

    public void _run() throws Throwable {

      for(int iter=0;iter<NUM_TEST_ITER;iter++) {
        final FieldData field = fields[TestCodecs.this.nextInt(fields.length)];
        if (Codec.DEBUG) {
          System.out.println("verify field=" + field.fieldInfo.name);
        }

        final TermsEnum termsEnum = termsDict.terms(field.fieldInfo.name).iterator();

        // Test straight enum of the terms:
        if (Codec.DEBUG) {
          System.out.println("\nTEST: pure enum");
        }

        int upto = 0;
        while(true) {
          final BytesRef term = termsEnum.next();
          if (term == null) {
            break;
          }
          if (Codec.DEBUG) {
            System.out.println("check " + upto + ": " + field.terms[upto].text2);
          }
          assertTrue(new BytesRef(field.terms[upto++].text2).bytesEquals(term));
        }
        assertEquals(upto, field.terms.length);

        // Test random seek:
        if (Codec.DEBUG) {
          System.out.println("\nTEST: random seek");
        }
        TermData term = field.terms[TestCodecs.this.nextInt(field.terms.length)];
        TermsEnum.SeekStatus status = termsEnum.seek(new BytesRef(term.text2));
        assertEquals(status, TermsEnum.SeekStatus.FOUND);
        assertEquals(term.docs.length, termsEnum.docFreq());
        if (field.omitTF) {
          this.verifyDocs(term.docs, term.positions, termsEnum.docs(null, null), false);
        } else {
          this.verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
        }

        // Test random seek by ord:
        final int idx = TestCodecs.this.nextInt(field.terms.length);
        term = field.terms[idx];
        status = termsEnum.seek(idx);
        assertEquals(status, TermsEnum.SeekStatus.FOUND);
        assertTrue(termsEnum.term().bytesEquals(new BytesRef(term.text2)));
        assertEquals(term.docs.length, termsEnum.docFreq());
        if (field.omitTF) {
          this.verifyDocs(term.docs, term.positions, termsEnum.docs(null, null), false);
        } else {
          this.verifyDocs(term.docs, term.positions, termsEnum.docsAndPositions(null, null), true);
        }

        // Test seek to non-existent terms:
        if (Codec.DEBUG)
          System.out.println("\nTEST: seek to non-existent term");
        for(int i=0;i<100;i++) {
          final char[] text = TestCodecs.this.getRandomText();
          final String text2 = new String(text, 0, text.length-1) + ".";
          status = termsEnum.seek(new BytesRef(text2));
          assertTrue(status == TermsEnum.SeekStatus.NOT_FOUND ||
                     status == TermsEnum.SeekStatus.END);
        }

        // Seek to each term, backwards:
        if (Codec.DEBUG) {
          System.out.println("\n" + Thread.currentThread().getName() + ": TEST: seek backwards through terms");
        }
        for(int i=field.terms.length-1;i>=0;i--) {
          if (Codec.DEBUG) {
            System.out.println(Thread.currentThread().getName() + ": TEST: term=" + field.terms[i].text2 + " has docFreq=" + field.terms[i].docs.length);
          }
          assertEquals(Thread.currentThread().getName() + ": field=" + field.fieldInfo.name + " term=" + field.terms[i].text2, TermsEnum.SeekStatus.FOUND, termsEnum.seek(new BytesRef(field.terms[i].text2)));
          assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
        }

        // Seek to each term by ord, backwards
        if (Codec.DEBUG) {
          System.out.println("\n" + Thread.currentThread().getName() + ": TEST: seek backwards through terms, by ord");
        }
        for(int i=field.terms.length-1;i>=0;i--) {
          if (Codec.DEBUG) {
            System.out.println(Thread.currentThread().getName() + ": TEST: term=" + field.terms[i].text2 + " has docFreq=" + field.terms[i].docs.length);
          }
          assertEquals(Thread.currentThread().getName() + ": field=" + field.fieldInfo.name + " term=" + field.terms[i].text2, TermsEnum.SeekStatus.FOUND, termsEnum.seek(i));
          assertEquals(field.terms[i].docs.length, termsEnum.docFreq());
          assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[i].text2)));
        }

        // Seek to non-existent empty-string term
        status = termsEnum.seek(new BytesRef(""));
        assertNotNull(status);
        assertEquals(status, TermsEnum.SeekStatus.NOT_FOUND);

        // Make sure we're now pointing to first term
        assertTrue(termsEnum.term().bytesEquals(new BytesRef(field.terms[0].text2)));

        // Test docs enum
        if (Codec.DEBUG) {
          System.out.println("\nTEST: docs/positions");
        }
        termsEnum.seek(new BytesRef(""));
        upto = 0;
        do {
          term = field.terms[upto];
          if (TestCodecs.this.nextInt(3) == 1) {
            if (Codec.DEBUG) {
              System.out.println("\nTEST [" + TestCodecs.this.getDesc(field, term) + "]: iterate docs...");
            }
            final DocsEnum docs = termsEnum.docs(null, null);
            final DocsAndPositionsEnum postings = termsEnum.docsAndPositions(null, null);

            final DocsEnum docsEnum;
            if (postings != null) {
              docsEnum = postings;
            } else {
              docsEnum = docs;
            }
            int upto2 = -1;
            while(upto2 < term.docs.length-1) {
              // Maybe skip:
              final int left = term.docs.length-upto2;
              int doc;
              if (TestCodecs.this.nextInt(3) == 1 && left >= 1) {
                final int inc = 1+TestCodecs.this.nextInt(left-1);
                upto2 += inc;
                if (Codec.DEBUG) {
                  System.out.println("TEST [" + TestCodecs.this.getDesc(field, term) + "]: skip: " + left + " docs left; skip to doc=" + term.docs[upto2] + " [" + upto2 + " of " + term.docs.length + "]");
                }

                if (TestCodecs.this.nextInt(2) == 1) {
                  doc = docsEnum.advance(term.docs[upto2]);
                  assertEquals(term.docs[upto2], doc);
                } else {
                  doc = docsEnum.advance(1+term.docs[upto2]);
                  if (doc == DocIdSetIterator.NO_MORE_DOCS) {
                    // skipped past last doc
                    assert upto2 == term.docs.length-1;
                    break;
                  } else {
                    // skipped to next doc
                    assert upto2 < term.docs.length-1;
                    if (doc >= term.docs[1+upto2]) {
                      upto2++;
                    }
                  }
                }
              } else {
                doc = docsEnum.nextDoc();
                assertTrue(doc != -1);
                if (Codec.DEBUG) {
                  System.out.println("TEST [" + TestCodecs.this.getDesc(field, term) + "]: got next doc...");
                }
                upto2++;
              }
              assertEquals(term.docs[upto2], doc);
              if (!field.omitTF) {
                assertEquals(term.positions[upto2].length, docsEnum.freq());
                if (TestCodecs.this.nextInt(2) == 1) {
                  if (Codec.DEBUG) {
                    System.out.println("TEST [" + TestCodecs.this.getDesc(field, term, term.docs[upto2]) + "]: check positions for doc " + term.docs[upto2] + "...");
                  }
                  this.verifyPositions(term.positions[upto2], postings);
                } else if (Codec.DEBUG) {
                  System.out.println("TEST: skip positions...");
                }
              } else if (Codec.DEBUG) {
                System.out.println("TEST: skip positions: omitTF=true");
              }
            }

            assertEquals(DocIdSetIterator.NO_MORE_DOCS, docsEnum.nextDoc());

          } else if (Codec.DEBUG) {
            System.out.println("\nTEST [" + TestCodecs.this.getDesc(field, term) + "]: skip docs");
          }
          upto++;

        } while (termsEnum.next() != null);

        assertEquals(upto, field.terms.length);
      }
    }
  }

  private void write(final FieldInfos fieldInfos, final Directory dir, final FieldData[] fields) throws Throwable {

    final int termIndexInterval = this.nextInt(13, 27);

    final SegmentWriteState state = new SegmentWriteState(null, dir, SEGMENT, fieldInfos, null, 10000, 10000, termIndexInterval,
                                                    CodecProvider.getDefault());

    final FieldsConsumer consumer = state.codec.fieldsConsumer(state);
    Arrays.sort(fields);
    for (final FieldData field : fields) {
      field.write(consumer);
    }
    consumer.close();
  }
}
