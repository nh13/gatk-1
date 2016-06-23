package org.broadinstitute.hellbender.engine;

import com.google.common.annotations.VisibleForTesting;
import genomicsdb.GenomicsDBFeatureReader;
import htsjdk.tribble.AbstractFeatureReader;
import htsjdk.tribble.Feature;
import htsjdk.tribble.FeatureCodec;
import htsjdk.tribble.FeatureReader;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static htsjdk.samtools.SAMFileHeader.GroupOrder.query;
import static htsjdk.samtools.util.SequenceUtil.n;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Method.get;

/**
 * Class to represent a Feature-containing input file. Tools should declare @Argument-annotated fields of
 * this type (or Collections of this type), and the Feature management system will automatically discover
 * them at runtime (provided that they are declared in the tool itself, a superclass of the tool, or an
 * ArgumentCollection of the tool).
 *
 * DO NOT ATTEMPT TO INSTANTIATE THIS CLASS DIRECTLY! FeatureInputs must be instantiated by the argument-parsing
 * system only in order to be recognized by the Feature management system. This is why the constructor is
 * marked as protected.
 *
 * FeatureInputs can be assigned logical names on the command line using the syntax:
 *
 *     --argument_name logical_name:feature_file
 *
 * These logical names can then be retrieved by the tool at runtime via {@link #getName}
 *
 * Furthermore, a list of comma-separated key=value pairs may be provided as follows:
 *
 *     --argument_name logical_name,key1=value1,key2=value2:feature_file
 *
 * the string value provided for a given key can be retrieved via {@link #getAttribute(String)}. Keys must be unique.
 *
 * @param <T> the type of Feature that this FeatureInput file contains (eg., VariantContext, BEDFeature, etc.)
 */
public final class FeatureInput<T extends Feature> {

    /**
     * Logical name for this source of Features optionally provided by the user on the command line
     * using the --argument_name logical_name:feature_file syntax. Defaults to the absolute path of
     * the underlying file if no logical name is specified
     */
    private final String name;

    private final Map<String, String> kevValueMap;

    private final FeatureCodec<T,?> codec;

    /**
     * File containing Features as specified by the user on the command line
     */
    private final String featureFile;

    /**
     * Type of Feature in the featureFile. Set manually by the engine after construction.
     */
    private Class<? extends Feature> featureType;

    private Boolean hasIndex = null;

    /**
     * Delimiter between the logical name and the file name in the --argument_name logical_name:feature_file syntax
     */
    public static final String FEATURE_ARGUMENT_TAG_DELIMITER = ":";

    /**
     * Delimiter between key-value pairs in the --argument_name logical_name,key1=value1,key2=value2:feature_file syntax.
     */
    public static final String FEATURE_ARGUMENT_KEY_VALUE_PAIR_DELIMITER = ",";

    /**
     * Separator between keys and values in the --argument_name logical_name,key1=value1,key2=value2:feature_file syntax.
     */
    public static final String FEATURE_ARGUMENT_KEY_VALUE_SEPARATOR = "=";

    FeatureCodec<T, ?> getCodec() {
        return (FeatureCodec<T, ?>) FeatureManager.getCodecForFile(getFeatureFile());
    }

    public <SOURCE> FeatureReader<T> getFeatureReader() {
        if (isTileDBInput()) {
            return makeGenomicsDBReader();
        } else {
            return AbstractFeatureReader.getFeatureReader(getFeatureFile().getAbsolutePath(), getCodec(), false);
        }
    }

    private <SOURCE> GenomicsDBFeatureReader<T, SOURCE> makeGenomicsDBReader() {



        return new GenomicsDBFeatureReader<T,SOURCE>();
    }

    private static class GenomicsDBInput {
        private final File loaderJson;
        private final File queryJson;

        public GenomicsDBInput(String path){
            if( !FeatureInput.isTileDBPath(path)){
                throw new GATKException("Trying to create a GenomicsDBReader from a non-genomics db input");
            }

            final String noheader = path.replace("gendb://", "");
            loaderJson = new File(noheader, "loader.json");
            queryJson = new File(noheader, "query.json");

            assertJsonExists(loaderJson);
            assertJsonExists(queryJson);
        }

        public FeatureReader<?>
        private static void assertJsonExists(File json) {
            if(!json.exists()) {
                throw new UserException("Couldn't connect to GenomicsDB because " + json.getAbsolutePath() + " does not exist.");
            }
        }
    }

    private boolean isTileDBInput() {
        return isTileDBPath(featureFile);
    }


    boolean readersHaveIndex(){
        if (isTileDBInput()){
            return true;
        } else if (hasIndex == null) {
                try (AbstractFeatureReader<?, ?> reader = AbstractFeatureReader.getFeatureReader(getFeatureFile().getAbsolutePath(), getCodec(), false)) {
                    hasIndex = reader.hasIndex();
                } catch (final IOException e) {
                    throw new GATKException("Failed to close a reader after looking for an index", e);
                }
        }
        return hasIndex;
    }

    /**
     * Represents a parsed argument for the FeatureInput.
     * Always has a file and a name.
     * May have attributes.
     */
    private static final class ParsedArgument{
        private final Map<String, String> keyValueMap;
        private final String name;
        private final String file;

        /**
         * Parses an argument value String of the forms:
         * "logical_name(,key=value)*:feature_file" or
         * "logical_name:feature_file" or
         * "feature_file"
         * into logical name and file name and key=value pairs.
         *
         * The absolute path of the file is used as the logical name if none is present.
         *
         * @param rawArgumentValue argument value from the command line to parse
         * @return The argument parsed from the provided string.
         */
        public static ParsedArgument of(final String rawArgumentValue) {
            final String[] tokens = rawArgumentValue.split(FEATURE_ARGUMENT_TAG_DELIMITER, -1);
            final String usage = "Argument must either be a file, or of the form logical_name:file or logical_name(,key=value)*:feature_file";

            // Check for malformed argument values
            if (tokens.length > 2 || tokens.length == 0) {
                throw new UserException.BadArgumentValue("", rawArgumentValue, usage);
            }
            for (final String token : tokens) {
                if (token.isEmpty()) {
                    throw new UserException.BadArgumentValue("", rawArgumentValue, "Empty name/file encountered. " + usage);
                }
            }

            if (tokens.length == 1) {
                // No user-specified logical name for this FeatureInput, so use the absolute path to the File as its name
                final String featureFile = tokens[0];
                return new ParsedArgument(featureFile.getAbsolutePath(), featureFile);
            }

            // User specified a logical name (and optional list of key-value pairs)
            // for this FeatureInput using name(,key=value)*:File syntax.
            // eg foo:file.vcf
            // eg foo,a=3,b=false,c=fred:file.vcf
            final String[] subtokens= tokens[0].split(FEATURE_ARGUMENT_KEY_VALUE_PAIR_DELIMITER, -1);
            if (subtokens[0].isEmpty()){
                throw new UserException.BadArgumentValue("", rawArgumentValue, usage);
            }
            final ParsedArgument pa= new ParsedArgument(subtokens[0], tokens[1]);
            //note: starting from 1 because 0 is the name
            for (int i = 1; i < subtokens.length; i++){
                final String[] kv = subtokens[i].split(FEATURE_ARGUMENT_KEY_VALUE_SEPARATOR, -1);
                if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()){
                    throw new UserException.BadArgumentValue("", rawArgumentValue, usage);
                }
                if (pa.containsKey(kv[0])){
                    throw new UserException.BadArgumentValue("", rawArgumentValue, "Duplicate key " + kv[0] + "\n" + usage);
                }
                pa.addKeyValue(kv[0], kv[1]);
            }
            return pa;
        }

        private ParsedArgument(final String name, final String file) {
            this.name=name;
            this.file=file;
            this.keyValueMap = new LinkedHashMap<>(2);
        }

        public String getFilePath(){
            return file;
        }

        public String getName() {
            return name;
        }

        /**
         * Returns an immutable view of the key-value map.
         */
        public Map<String, String> keyValueMap() {
            return Collections.unmodifiableMap(keyValueMap);
        }

        public void addKeyValue(final String k, final String v) {
            keyValueMap.put(k, v);
        }

        private boolean containsKey(final String k) {
            return keyValueMap.containsKey(k);
        }
    }

    /**
     * Construct a FeatureInput from a String argument value either of the form "logical_name:feature_file"
     * or simply "feature_file".
     *
     * Only meant to be called by the argument parsing system, and therefore marked as package-visible --
     * FeatureInputs constructed some other way will not be recognized by the engine.
     *
     * Note: cannot delegate to another constructor because Java only allows a call to "this" on the first line of a constructor.
     *
     * @param rawArgumentValue String of the form "logical_name:feature_file" or "feature_file"
     */
    FeatureInput(final String rawArgumentValue) {
        Utils.nonNull(rawArgumentValue, "rawArgumentValue");
        final ParsedArgument parsedArgument = ParsedArgument.of(rawArgumentValue);

        name = parsedArgument.getName();
        kevValueMap = parsedArgument.keyValueMap();
        featureFile = parsedArgument.getFilePath();
        featureType = null;  // Must be set after construction
        codec = null;
    }

    private static boolean isTileDBPath(String filePath){
        return filePath.startsWith("gendb://");
    }

    /**
     * Construct a FeatureInput from raw components: name, key value pairs and the file.
     *
     * This constructor is meant to be called by the engine and test classes --
     * FeatureInputs constructed some other way will not be recognized by the engine.
     */
    @VisibleForTesting
    public FeatureInput(final String name, final Map<String, String> kevValueMap, final File featureFile) {
        Utils.nonNull(name, "name");
        Utils.nonNull(kevValueMap, "kevValueMap");
        Utils.nonNull(featureFile, "featureFile");
        this.name = name;
        this.kevValueMap = Collections.unmodifiableMap(new LinkedHashMap<>(kevValueMap));   //make a unmodifiable copy
        this.featureFile = featureFile;
        this.featureType = null;  // Must be set after construction
        this.codec = null;
    }

    public FeatureInput(final String name, final FeatureCodec<T,?> codec, final File featureFile){
        Utils.nonNull(name, "name");
        Utils.nonNull(codec, "codec");
        Utils.nonNull(featureFile, "featureFile");
        this.name = name;
        this.kevValueMap = Collections.emptyMap();
        this.featureFile = featureFile;
        this.featureType = null;
        this.codec = null;
    }

    /**
     * Gets the value for the given key associated with this Feature source or {@code null}
     * if no value is associated with a given key.
     * @throws IllegalArgumentException if the key is {@code null}.
     */
    public String getAttribute(final String key) {
        Utils.nonNull(key);
        return kevValueMap.get(key);
    }

    /**
     * Gets the logical name of this Feature source. This will be a user-provided value if the
     * --argument_name logical_name:feature_file was used on the command line, otherwise it will
     * default to the absolute path of the backing file
     *
     * @return logical name of this source of Features
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the file backing this source of Features
     *
     * @return file backing this source of Features
     */
    public File getFeatureFile() {
        return featureFile;
    }

    /**
     * Gets the type of Feature contained in our file
     *
     * @return the type of Feature contained in our file
     */
    public Class<? extends Feature> getFeatureType() {
        return featureType;
    }

    /**
     * Sets the type of Feature contained in our file. Called by the engine after construction time.
     *
     * @param featureType the type of Feature contained in our file
     */
    protected void setFeatureType(final Class<? extends Feature> featureType) {
        this.featureType = featureType;
    }

    /**
     * FeatureInputs will be hashed by the engine, so make an effort to produce a reasonable hash code
     *
     * @return hash code for this FeatureInput (combination of hash code of the name and file)
     */
    @Override
    public int hashCode() {
        return 31 * name.hashCode() + featureFile.hashCode();
    }

    /**
     * Test FeatureInputs for equality
     *
     * @param other object to compare this FeatureInput with
     * @return true if this FeatureInput equals other, otherwise false
     */
    @Override
    public boolean equals(final Object other) {
        if (! (other instanceof FeatureInput)) {
            return false;
        }

        final FeatureInput<?> otherFeature = (FeatureInput<?>)other;
        return name.equals(otherFeature.name) && featureFile.equals(otherFeature.featureFile);
    }

    /**
     * Returns a String representation of this FeatureInput. Will be the absolute path to
     * the featureFile if we have no logical name, or a String of the form
     * "logical_name:absolute_path_to_featureFile" if we do have a logical name.
     *
     * @return String representation of this FeatureInput
     */
    @Override
    public String toString() {
        final String featureFilePath = featureFile;
        return name.equals(featureFilePath) ? featureFilePath :
                                              String.format("%s%s%s", name, FEATURE_ARGUMENT_TAG_DELIMITER, featureFilePath);
    }
}
