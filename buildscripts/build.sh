#!/usr/bin/env bash

set -e
set -u

function usage() {
    echo 1>&2 "build.sh"
    echo 1>&2 "[--mongo=$mongo] [--ft_index=$ft_index] [--jemalloc=$jemalloc]"
    echo 1>&2 "[--github_user=$github_user] [--github_token=$github_token] [--github_use_ssh=$github_use_ssh]"
    echo 1>&2 "[--cc=$cc --cxx=$cxx] [--ftcc=$ftcc --ftcxx=$ftcxx]"
    echo 1>&2 "[--debugbuild=$debugbuild]"
    return 1
}

function retry() {
    set +e
    local cmd
    local retries
    local exitcode
    cmd=$*
    let retries=0
    while [ $retries -le 10 ] ; do
        echo `date` $cmd
        bash -c "$cmd"
        exitcode=$?
        echo `date` $cmd $exitcode $retries
        let retries=retries+1
        if [ $exitcode -eq 0 ] ; then break; fi
        sleep 10
    done
    set -e
    test $exitcode = 0
}

function get_ncpus() {
    if [ -f /proc/cpuinfo ]; then
        grep bogomips /proc/cpuinfo | wc -l
    else
        sysctl -n hw.ncpu
    fi
}

function github_download() {
    repo=$1; shift
    rev=$1; shift
    dest=$1; shift
    mkdir $dest

    if [ ! -z $github_token ] ; then
        retry curl \
            --header "Authorization:\\ token\\ $github_token" \
            --location https://api.github.com/repos/$repo/tarball/$rev \
            --output $dest.tar.gz
        tar --extract \
            --gzip \
            --directory=$dest \
            --strip-components=1 \
            --file $dest.tar.gz
        rm -f $dest.tar.gz
    elif [ ! -z $github_user ] ; then
        retry curl \
            --user $github_user \
            --location https://api.github.com/repos/$repo/tarball/$rev \
            --output $dest.tar.gz
        tar --extract \
            --gzip \
            --directory=$dest \
            --strip-components=1 \
            --file $dest.tar.gz
        rm -f $dest.tar.gz
    elif [ $github_use_ssh != 0 ] ; then
        tempdir=$(mktemp -d -p $PWD)
        retry git clone git@github.com:${repo}.git $tempdir

        # export the right branch or tag
        (cd $tempdir ;
            git archive \
                --format=tar \
                $rev) | \
            tar --extract \
                --directory $dest

        rm -rf $tempdir
    else
        retry curl \
            --location https://api.github.com/repos/$repo/tarball/$rev \
            --output $dest.tar.gz
        tar --extract \
            --gzip \
            --directory=$dest \
            --strip-components=1 \
            --file $dest.tar.gz
        rm -f $dest.tar.gz
    fi
}

# check out the fractal tree source from subversion, build it, and make the fractal tree tarballs
function build_fractal_tree() {
    if [ ! -d $tokufractaltreedir ] ; then
        mkdir $tokufractaltreedir

        if [ ! -d ft-index ] ; then
            github_download Tokutek/ft-index $ft_index ft-index
            github_download Tokutek/jemalloc $jemalloc ft-index/third_party/jemalloc
        fi

        pushd ft-index
            echo `date` make ft-index $ftcc $($ftcc --version)
            cmake_env="CC=$ftcc CXX=$ftcxx"
            local build_type=""
            local use_valgrind=""
            local debug_paranoid=""
            if [[ $debugbuild = 1 ]]; then
                build_type="Debug"
                use_valgrind="ON"
                debug_paranoid="ON"
            else
                build_type="Release"
                use_valgrind="OFF"
                debug_paranoid="OFF"
            fi
            mkdir -p build
            cd build
            eval $cmake_env cmake \
                -D LIBTOKUDB=$tokufractaltree \
                -D LIBTOKUPORTABILITY=$tokuportability \
                -D CMAKE_TOKUDB_REVISION=0 \
                -D CMAKE_BUILD_TYPE=$build_type \
                -D CMAKE_INSTALL_PREFIX=$rootdir/$tokufractaltreedir \
                -D JEMALLOC_SOURCE_DIR=../third_party/jemalloc \
                -D BUILD_TESTING=OFF \
                -D USE_GTAGS=OFF \
                -D USE_CTAGS=OFF \
                -D USE_ETAGS=OFF \
                -D USE_CSCOPE=OFF \
                -D USE_VALGRIND=$use_valgrind \
                -D TOKU_DEBUG_PARANOID=$debug_paranoid \
                ..
            make install -j$makejobs
        popd

        pushd $tokufractaltreedir/examples
            # test the examples
            sed -ie "s/LIBTOKUDB = tokudb/LIBTOKUDB = $tokufractaltree/" Makefile 
            sed -ie "s/LIBTOKUPORTABILITY = tokuportability/LIBTOKUPORTABILITY = $tokuportability/" Makefile
            if [ x"$(uname)" = x"Darwin" ] ; then
                set +u
                DYLD_LIBRARY_PATH=$PWD/../lib:$DYLD_LIBRARY_PATH make check CC=$ftcc
                set -u
                exitcode=$?
            else
                make check CC=$ftcc
                exitcode=$?
            fi
            echo `date` make check examples $tokufractaltree $exitcode
            make clean
        popd

        # make tarballs
        tar --create \
            --gzip \
            --file $tokufractaltreedir.tar.gz \
            $tokufractaltreedir
        md5sum $tokufractaltreedir.tar.gz >$tokufractaltreedir.tar.gz.md5
        md5sum --check $tokufractaltreedir.tar.gz.md5
    fi
}

# checkout the mongodb source from git, generate a build script, and make the mongodb source tarball
function build_mongodb_src() {
    mongodbsrc=mongodb-$mongodb_version-tokutek-$mongo_rev-src
    if [ ! -d $mongodbsrc ] ; then
        github_download Tokutek/mongo $mongo $mongodbsrc

        # set defaults for build script
        sed <$mongodbsrc/buildscripts/build.tokudb.sh.in \
            -e "s^@makejobs@^$makejobs^" \
            -e "s^@cc@^$cc^" \
            -e "s^@cxx@^$cxx^" \
            -e "s^@debugbuild@^$debugbuild^" \
            -e "s^@force_git_version@^$mongo_rev^" \
            -e "s^@force_toku_version@^$ft_index_rev^" \
            -e "s^@mongodbsrc@^$mongodbsrc^" \
            -e "s^@tokufractaltreesrc@^$tokufractaltreedir^" \
            -e "s^@LIBTOKUDB_NAME@^${tokufractaltree}_static^" \
            -e "s^@LIBTOKUPORTABILITY_NAME@^${tokuportability}_static^" \
            >$mongodbsrc/buildscripts/build.tokudb.sh
        chmod +x $mongodbsrc/buildscripts/build.tokudb.sh

        # make the mongodb src tarball
        tar --create \
            --gzip \
            --file $mongodbsrc.tar.gz \
            $mongodbsrc
        md5sum $mongodbsrc.tar.gz >$mongodbsrc.tar.gz.md5
        md5sum --check $mongodbsrc.tar.gz.md5

        # run the build script
        $mongodbsrc/buildscripts/build.tokudb.sh

        mongodbdir=mongodb-$mongodb_version-tokutek-$mongo_rev-tokudb-${ft_index_rev}${suffix}-$system-$arch

        if [ -f $mongodbsrc/mongodb*-debuginfo.tgz ]; then
            # copy the debuginfo tarball to a name of our choosing
            mkdir $mongodbdir-debuginfo
            tar --extract \
                --gzip \
                --directory $mongodbdir-debuginfo \
                --strip-components 1 \
                --file $mongodbsrc/mongodb*-debuginfo.tgz
            tar --create \
                --gzip \
                --file $mongodbdir-debuginfo.tar.gz \
                $mongodbdir-debuginfo
            md5sum $mongodbdir-debuginfo.tar.gz >$mongodbdir-debuginfo.tar.gz.md5
            md5sum --check $mongodbdir-debuginfo.tar.gz.md5

            # now remove it so the next file glob doesn't get confused
            rm $mongodbsrc/mongodb*-debuginfo.tgz
        fi

        # copy the release tarball to a name of our choosing
        mkdir $mongodbdir
        tar --extract \
            --gzip \
            --directory $mongodbdir \
            --strip-components 1 \
            --file $mongodbsrc/mongodb*.tgz
        tar --create \
            --gzip \
            --file $mongodbdir.tar.gz \
            $mongodbdir
        md5sum $mongodbdir.tar.gz >$mongodbdir.tar.gz.md5
        md5sum --check $mongodbdir.tar.gz.md5
    fi
}

PATH=$HOME/bin:$PATH

suffix=''
mongodb_version=2.2.4
mongo=master
ft_index=master
jemalloc=3.3.1
cc=gcc44
cxx=g++44
ftcc=gcc47
ftcxx=g++47
system=`uname -s | tr '[:upper:]' '[:lower:]'`
arch=`uname -m | tr '[:upper:]' '[:lower:]'`
github_user=''
github_token=''
github_use_ssh=0
makejobs=$(get_ncpus)
debugbuild=0
staticft=1

if [ $(uname -s) = Darwin ] ; then
    cc=cc
    cxx=c++
    ftcc=cc
    ftcxx=c++
fi

if ! command -v $cc &>/dev/null ; then
    cc=cc
    cxx=c++
fi
if ! command -v $ftcc &>/dev/null ; then
    ftcc=cc
    ftcxx=c++
fi

while [ $# -gt 0 ] ; do
    arg=$1; shift
    if [ $arg = "--gcc44" ] ; then
        cc=gcc44; cxx=g++44
    elif [[ $arg =~ --(.*)=(.*) ]] ; then
        eval ${BASH_REMATCH[1]}=${BASH_REMATCH[2]}
    else
        usage; exit 1;
    fi
done

# Check for scons, the interpreter mongo uses for builds
set +e
command -v scons &>/dev/null
if [ $? != 0 ] ; then
    echo "Need scons to build MongoDB!"
    exit 1;
fi
set -e

if [[ $debugbuild != 0 && ( -z $suffix ) ]] ; then suffix=-debug; fi

if [ ! -z $github_user ] ; then
    ft_index_rev=$(git ls-remote https://$github_user@github.com/Tokutek/ft-index.git $ft_index | cut -c-7)
    mongo_rev=$(git ls-remote https://$github_user@github.com/Tokutek/mongo.git $mongo | cut -c-7)
elif [ ! -z $github_token ] ; then
    ft_index_rev=$(git ls-remote https://${github_token}:x-oauth-basic@github.com/Tokutek/ft-index.git $ft_index | cut -c-7)
    mongo_rev=$(git ls-remote https://${github_token}:x-oauth-basic@github.com/Tokutek/mongo.git $mongo | cut -c-7)
elif [ $github_use_ssh != 0 ] ; then
    ft_index_rev=$(git ls-remote git@github.com:Tokutek/ft-index.git $ft_index | cut -c-7)
    mongo_rev=$(git ls-remote git@github.com:Tokutek/mongo.git $mongo | cut -c-7)
else
    ft_index_rev=$(git ls-remote http://github.com/Tokutek/ft-index.git $ft_index | cut -c-7)
    mongo_rev=$(git ls-remote http://github.com/Tokutek/mongo.git $mongo | cut -c-7)
fi

# maybe they just passed a rev, not a branch or tag
if [ ! -z $ft_index_rev ] ; then
    ft_index_rev=$ft_index
fi
if [ ! -z $mongo_rev ] ; then
    mongo_rev=$mongo
fi

builddir=build-mongodb-${mongo_rev}-tokudb-${ft_index_rev}${suffix}
if [ ! -d $builddir ] ; then mkdir $builddir; fi
pushd $builddir

rootdir=$PWD

# build the fractal tree tarball
tokufractaltree=tokufractaltreeindex-${ft_index_rev}${suffix}
tokuportability=tokuportability-${ft_index_rev}${suffix}
tokufractaltreedir=$tokufractaltree-$system-$arch
build_fractal_tree

# build the mongodb source tarball
build_mongodb_src

popd

exit 0
