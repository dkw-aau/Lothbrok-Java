package org.linkeddatafragments.fragments.spf;

import org.lothbrok.utils.RandomString;

public class StarPatternElementFactory<CTT,NVT,AVT>
{

    /**
     *
     * @return
     */
    public IStarPatternElement<CTT,NVT,AVT> createUnspecifiedVariable()
    {
        return new UnspecifiedVariable<CTT,NVT,AVT>();
    }

    /**
     *
     * @param v
     * @return
     */
    public IStarPatternElement<CTT,NVT,AVT> createNamedVariable( final NVT v )
    {
        return new NamedVariable<CTT,NVT,AVT>( v );
    }

    /**
     *
     * @param bnode
     * @return
     */
    public IStarPatternElement<CTT,NVT,AVT> createAnonymousVariable(
            final AVT bnode )
    {
        return new AnonymousVariable<CTT,NVT,AVT>( bnode );
    }

    /**
     *
     * @param term
     * @return
     */
    public IStarPatternElement<CTT,NVT,AVT> createConstantRDFTerm(
            final CTT term )
    {
        return new ConstantRDFTerm<CTT,NVT,AVT>( term );
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static abstract public class Variable<CTT,NVT,AVT>
            implements IStarPatternElement<CTT,NVT,AVT>
    {
        @Override
        public boolean isVariable() { return true; }
        @Override
        public CTT asConstantTerm() { throw new UnsupportedOperationException(); }
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static public class UnspecifiedVariable<CTT,NVT,AVT>
            extends Variable<CTT,NVT,AVT>
    {
        private final String var;
        public UnspecifiedVariable() {
            RandomString rs = new RandomString(5);
            this.var = rs.nextString();
        }
        @Override
        public boolean isSpecificVariable() { return false; }
        @Override
        public boolean isNamedVariable() { return false; }
        @Override
        public NVT asNamedVariable() { throw new UnsupportedOperationException(); }
        @Override
        public boolean isAnonymousVariable() { return false; }
        @Override
        public AVT asAnonymousVariable() { throw new UnsupportedOperationException(); }

        @Override
        public boolean isUnspecifiedVariable() {
            return true;
        }

        @Override
        public String asUnspecifiedVariable() throws UnsupportedOperationException {
            return var;
        }

        @Override
        public String toString() { return "UnspecifiedVariable(" + var + ")"; }
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static abstract public class SpecificVariable<CTT,NVT,AVT>
            extends Variable<CTT,NVT,AVT>
    {
        @Override
        public boolean isSpecificVariable() { return true; }
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static public class NamedVariable<CTT,NVT,AVT>
            extends SpecificVariable<CTT,NVT,AVT>
    {

        /**
         *
         */
        protected final NVT v;

        /**
         *
         * @param variable
         */
        public NamedVariable( final NVT variable ) { v = variable; }
        @Override
        public boolean isNamedVariable() { return true; }
        @Override
        public NVT asNamedVariable() { return v; }
        @Override
        public boolean isAnonymousVariable() { return false; }
        @Override
        public AVT asAnonymousVariable() { throw new UnsupportedOperationException(); }

        @Override
        public boolean isUnspecifiedVariable() {
            return false;
        }

        @Override
        public String asUnspecifiedVariable() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() { return "NamedVariable(" + v.toString() + ")"; }
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static public class AnonymousVariable<CTT,NVT,AVT>
            extends SpecificVariable<CTT,NVT,AVT>
    {

        /**
         *
         */
        protected final AVT bn;

        /**
         *
         * @param bnode
         */
        public AnonymousVariable( final AVT bnode ) { bn = bnode; }
        @Override
        public boolean isNamedVariable() { return false; }
        @Override
        public NVT asNamedVariable() { throw new UnsupportedOperationException(); }
        @Override
        public boolean isAnonymousVariable() { return true; }
        @Override
        public AVT asAnonymousVariable() { return bn; }

        @Override
        public boolean isUnspecifiedVariable() {
            return false;
        }

        @Override
        public String asUnspecifiedVariable() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() { return "AnonymousVariable(" + bn.toString() + ")"; }
    }

    /**
     *
     * @param <CTT>
     * @param <NVT>
     * @param <AVT>
     */
    static public class ConstantRDFTerm<CTT,NVT,AVT>
            implements IStarPatternElement<CTT,NVT,AVT>
    {

        /**
         *
         */
        protected final CTT t;

        /**
         *
         * @param term
         */
        public ConstantRDFTerm( final CTT term ) { t = term; }
        @Override
        public boolean isVariable() { return false; }
        @Override
        public boolean isSpecificVariable() { return false; }
        @Override
        public boolean isNamedVariable() { return false; }
        @Override
        public NVT asNamedVariable() { throw new UnsupportedOperationException(); }
        @Override
        public boolean isAnonymousVariable() { return false; }
        @Override
        public AVT asAnonymousVariable() { throw new UnsupportedOperationException(); }
        @Override
        public CTT asConstantTerm() { return t; }
        @Override
        public boolean isUnspecifiedVariable() {
            return false;
        }

        @Override
        public String asUnspecifiedVariable() throws UnsupportedOperationException {
            throw new UnsupportedOperationException();
        }
        @Override
        public String toString() { return "ConstantRDFTerm(" + t.toString() + ")(type: " + t.getClass().getSimpleName() + ")"; }
    }

}
