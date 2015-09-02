/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.math.util.MathUtils;

import xerial.larray.LByteArray;
import xerial.larray.japi.LArrayJ;
import me.lemire.integercompression.BitPacking;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;


public class FixedBitSVReaderPerfBenchmark {

  public static void main(String[] args) throws IOException {
    int rows = 25000000;
    int columnSizeInBits = 3;
    boolean hasNulls = false;
    File file = new File(args[0]);
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    int length = (int) file.length();
    byte inmemory[] = new byte[length];
    ByteBuffer byteBuffer = ByteBuffer.wrap(inmemory);
    raf.getChannel().read(byteBuffer);
    byteBuffer.rewind();
    byte[] rawData = new byte[length];
    long start, end, count = 0;
    LByteArray lByteArray = LArrayJ.loadLByteArrayFrom(file);
    while (count++ < 10) {
      byteBuffer.rewind();
      start = System.currentTimeMillis();
      for (int i = 0; i < length; i++) {
        rawData[i] = lByteArray.getByte(i);
      }
      end = System.currentTimeMillis();
      System.out.println("Raw read took:" + (end - start));
    }
    byte[] rawDataCopy = new byte[length];
    count = 0;
    while (count++ < 10) {
      start = System.currentTimeMillis();
      for (int i = 0; i < length; i++) {
        rawDataCopy[i] = rawData[i];
      }
      end = System.currentTimeMillis();
      System.out.println("copying took :" + (end - start));
    }
    int[] bitMasks = new int[32 - columnSizeInBits];
    int allOneBit = (int) Math.pow(2, columnSizeInBits) - 1;
    for (int startPos = 0; startPos < bitMasks.length - columnSizeInBits; startPos++) {
      bitMasks[startPos] = Integer.rotateLeft(allOneBit, bitMasks.length - startPos);
    }
    int result[] = new int[rows];
    byteBuffer.rewind();
    for (int i = 0; i < rows; i++) {
      try {
        int startBitPos = i * columnSizeInBits;
        int startBytePos = (startBitPos + 7) / 8;
        byteBuffer.position(startBytePos);
        int val = byteBuffer.getInt();
        int bitOffset = startBitPos % bitMasks.length;
        result[i] = (val & bitMasks[bitOffset]) >>> (32 - (bitOffset + 3));
      } catch (Exception e) {
        System.out.println("Exception trying to get value for row:" + i);
      }
    }

    int outputSize = MathUtils.lcm(32, columnSizeInBits) / columnSizeInBits;
    int inputSize = MathUtils.lcm(32, columnSizeInBits) / 32;
    int input[] = new int[inputSize];
    Arrays.fill(input, 0);
    int output[] = new int[outputSize];
    start = System.currentTimeMillis();
    int result1[] = new int[rows];
    int destPos = 0;
    byteBuffer.rewind();
    for (int i = 0; i < length / (4 * inputSize); i++) {
      for (int j = 0; j < inputSize; j++) {
        input[j] = byteBuffer.getInt();
      }
      BitPacking.fastunpack(input, 0, output, 0, columnSizeInBits);
      //System.arraycopy(output, 0, result1, destPos, outputSize);
      //destPos +=outputSize;
    }
    end = System.currentTimeMillis();
    System.out.println("Time taken:" + (end - start));
    FixedBitCompressedSVForwardIndexReader reader =
        new FixedBitCompressedSVForwardIndexReader(file, rows, columnSizeInBits, ReadMode.HEAP, hasNulls);
    start = System.currentTimeMillis();
    int[] result2 = new int[rows];
    for (int i = 0; i < rows; i++) {
      result2[i] = reader.getInt(i);
    }
    end = System.currentTimeMillis();
    System.out.println("Time taken:" + (end - start));
    for (int i = 0; i < 100; i++) {
      System.out.println(result1[i] + ":" + result2[i] + ":" + result[i]);
    }
    reader.close();
  }
}
