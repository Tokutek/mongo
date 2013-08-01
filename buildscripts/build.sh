#!/usr/bin/env bash

set -e
set -u

function usage() {
    echo 1>&2 "build.sh"
    echo 1>&2 "[--mongo=$mongo] [--ft_index=$ft_index] [--jemalloc=$jemalloc] [--backup=$backup]"
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
function build_backup_lib() {
    if [ ! -d $tokubackupdir ] ; then
        mkdir $tokubackupdir

        if [ ! -d backup-community ] ; then
            github_download Tokutek/backup-community $backup_community_rev backup-community
        fi

        pushd backup-community
            echo `date` make backup-community $ftcc $($ftcc --version)
            cmake_env="CC=$ftcc CXX=$ftcxx"
            local build_type=""
            local use_valgrind=""
            local debug_paranoid=""
            if [[ $debugbuild = 1 ]]; then
                build_type="Debug"
            else
                build_type="Release"
            fi
            mkdir -p build
            cd build
            eval $cmake_env cmake \
                -D HOT_BACKUP_LIBNAME=HotBackup \
                -D CMAKE_BUILD_TYPE=$build_type \
                -D CMAKE_INSTALL_PREFIX=$rootdir/$tokubackupdir \
                -D BUILD_TESTING=OFF \
                -Wno-dev \
                ..
            make install -j$makejobs
        popd

        # make tarballs
        tar --create \
            --gzip \
            --file $tokubackupdir.tar.gz \
            $tokubackupdir
        md5sum $tokubackupdir.tar.gz >$tokubackupdir.tar.gz.md5
        md5sum --check $tokubackupdir.tar.gz.md5
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
                -D CMAKE_TOKUDB_REVISION=0x$ft_index_rev \
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

        if [ $tokukv_check != 0 ] ; then
            pushd $tokufractaltreedir/examples
                # test the examples
                sed -ie "s/LIBTOKUDB = tokufractaltree/LIBTOKUDB = $tokufractaltree/" Makefile 
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
        fi

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
    mongodbsrc=tokumx-$mongo-src
    if [ ! -d $mongodbsrc ] ; then
        github_download Tokutek/mongo $mongo $mongodbsrc

        # set defaults for build script
        sed <$mongodbsrc/buildscripts/build.tokukv.sh.in \
            -e "s^@makejobs@^$makejobs^" \
            -e "s^@cc@^$cc^" \
            -e "s^@cxx@^$cxx^" \
            -e "s^@debugbuild@^$debugbuild^" \
            -e "s^@force_git_version@^$mongo_rev^" \
            -e "s^@force_toku_version@^$ft_index_rev^" \
            -e "s^@mongodbsrc@^$mongodbsrc^" \
            -e "s^@tokufractaltreesrc@^$tokufractaltreedir^" \
            -e "s^@tokubackupsrc@^$tokubackupdir^" \
            -e "s^@LIBTOKUFRACTALTREE_NAME@^${tokufractaltree}^" \
            -e "s^@LIBTOKUPORTABILITY_NAME@^${tokuportability}^" \
            >$mongodbsrc/buildscripts/build.tokukv.sh
        chmod +x $mongodbsrc/buildscripts/build.tokukv.sh

        # make the mongodb src tarball
        tar --create \
            --gzip \
            --file $mongodbsrc.tar.gz \
            $mongodbsrc
        md5sum $mongodbsrc.tar.gz >$mongodbsrc.tar.gz.md5
        md5sum --check $mongodbsrc.tar.gz.md5

        # run the build script
        $mongodbsrc/buildscripts/build.tokukv.sh

        for tarball in $mongodbsrc/tokumx*.tgz
        do
            name=$(basename $tarball)
            cp $tarball $name
            md5sum $name >$name.md5
            md5sum --check $name.md5
        done
    fi
}

PATH=$HOME/bin:$PATH

suffix=''
mongodb_version=2.2.4
mongo=master
ft_index=master
backup=master
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
tokukv_check=0

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
    backup_community_rev=$(git ls-remote https://$github_user@github.com/Tokutek/backup-community.git $backup | cut -c-7)
    mongo_rev=$(git ls-remote https://$github_user@github.com/Tokutek/mongo.git $mongo | cut -c-7)
elif [ ! -z $github_token ] ; then
    ft_index_rev=$(git ls-remote https://${github_token}:x-oauth-basic@github.com/Tokutek/ft-index.git $ft_index | cut -c-7)
    backup_community_rev=$(git ls-remote https://${github_token}:x-oauth-basic@github.com/Tokutek/backup-community.git $backup | cut -c-7)
    mongo_rev=$(git ls-remote https://${github_token}:x-oauth-basic@github.com/Tokutek/mongo.git $mongo | cut -c-7)
elif [ $github_use_ssh != 0 ] ; then
    ft_index_rev=$(git ls-remote git@github.com:Tokutek/ft-index.git $ft_index | cut -c-7)
    backup_community_rev=$(git ls-remote git@github.com:Tokutek/backup-community.git $backup | cut -c-7)
    mongo_rev=$(git ls-remote git@github.com:Tokutek/mongo.git $mongo | cut -c-7)
else
    ft_index_rev=$(git ls-remote http://github.com/Tokutek/ft-index.git $ft_index | cut -c-7)
    backup_community_rev=$(git ls-remote http://github.com/Tokutek/backup-community.git $backup | cut -c-7)
    mongo_rev=$(git ls-remote http://github.com/Tokutek/mongo.git $mongo | cut -c-7)
fi

# maybe they just passed a rev, not a branch or tag
if [ -z $ft_index_rev ] ; then
    ft_index_rev=$ft_index
fi
if [ -z $backup_community_rev ] ; then
    backup_community_rev=$backup
fi
if [ -z $mongo_rev ] ; then
    mongo_rev=$mongo
fi

builddir=build-tokumx-${mongo_rev}-tokukv-${ft_index_rev}${suffix}
if [ ! -d $builddir ] ; then mkdir $builddir; fi
pushd $builddir

rootdir=$PWD

# build the fractal tree tarball
tokufractaltree=tokukv-${ft_index_rev}${suffix}
tokuportability=tokuportability-${ft_index_rev}${suffix}
tokufractaltreedir=$tokufractaltree-$system-$arch
build_fractal_tree
backup_community=backup-community-${backup_community_rev}${suffix}
backupdir=$backup_community-$system-$arch
build_backup

# build the mongodb source tarball
build_mongodb_src

popd

exit 0
