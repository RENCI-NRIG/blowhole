Summary: Blowhole - An RSpec manifest spouter
Name: blowhole
Version: 0.2
Release: 16
BuildRoot: %{_builddir}/%{name}-root
Source0: blowhole-0.2.tgz
Group: Applications/Communications
Vendor: ExoGENI
Packager: ExoGENI
License: GENI Public License
URL: https://geni-orca.renci.org/svn/orca-external/pubsub-daemon/trunk

BuildRequires:  jdk
Requires:       jdk

%define homedir /opt/blowhole
%define conf_dir %{_sysconfdir}/blowhole
%define log_dir %{_localstatedir}/log/blowhole
%define pid_dir %{_localstatedir}/run/blowhole
%define exogeni_user_id geni-orca
%define exogeni_group_id nonrenci
# couldn't find another way to disable the brp-java-repack-jars which was called in __os_install_post
%define __os_install_post %{nil}
# And this is needed to get around the generated wrapper binaries...
%global debug_package %{nil}

%description
Blowhole is an XMPP client that subscribes to individual manifests, converts them to RSpec,
and publishes them at the indicated URL.

%prep
%setup -q -n blowhole-0.2

%build
LANG=en_US.UTF-8 mvn clean package

%install
# Prep the install location.
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{homedir}
# Copy over the generated daemon
cp -R target/generated-resources/appassembler/jsw/blowholed/* $RPM_BUILD_ROOT%{homedir}
# Copy over the generated utilities and dependencies
cp -R target/appassembler/bin/* $RPM_BUILD_ROOT%{homedir}/bin
cp -R target/appassembler/repo $RPM_BUILD_ROOT%{homedir}
# Fix up generated daemon/init script
sed -i -e 's;su -m;su -s /bin/sh -m;' $RPM_BUILD_ROOT%{homedir}/bin/blowholed
sed -i -e 's;APP_NAME="blowholed";APP_NAME="blowholed"\nAPP_BASE=%{homedir};' $RPM_BUILD_ROOT%{homedir}/bin/blowholed
sed -i -e 's;^PIDDIR="$APP_BASE/logs";PIDDIR="%{pid_dir}";' $RPM_BUILD_ROOT%{homedir}/bin/blowholed
# Ilya says we don't need this anymore, and that log rotation is automatically handled.
#sed -i -e 's;wrapper.daemonize=TRUE;wrapper.logfile=\\"%{log_dir}/blowhole-stdout.log\\" wrapper.daemonize=TRUE;' $RPM_BUILD_ROOT%{homedir}/bin/blowholed
# Modify wrapper.conf to include local overrides file, for ease of management via puppet.
echo -e "# Finally, include overrides to things set in this file\n#include %{homedir}/conf/wrapper-overrides.conf\n" >> $RPM_BUILD_ROOT%{homedir}/conf/wrapper.conf
# Ensure the all utilities are executable.
chmod 755 $RPM_BUILD_ROOT%{homedir}/bin/*

# Create a config directory.
mkdir -p $RPM_BUILD_ROOT%{conf_dir}
# Create a log directory.
mkdir -p $RPM_BUILD_ROOT%{log_dir}
# Create a run directory to store pid files.
mkdir -p $RPM_BUILD_ROOT%{pid_dir}

# Clean up the bin and lib directories
rm -rf $RPM_BUILD_ROOT%{homedir}/bin/*.bat
rm -rf $RPM_BUILD_ROOT%{homedir}/bin/wrapper-macosx-universal-32
rm -rf $RPM_BUILD_ROOT%{homedir}/lib/libwrapper-macosx-universal-32.jnilib

%clean
rm -rf $RPM_BUILD_ROOT

%preun
if [ "$1" == "0" ]; then
	/sbin/chkconfig --del blowholed
	[ -x "/etc/init.d/blowholed" ] && /etc/init.d/blowholed stop
        rm -f /etc/init.d/blowholed
fi
# Force a successful exit even if we didn't exit cleanly.
exit 0

%post
rm -f /etc/init.d/blowholed
ln -s %{homedir}/bin/blowholed /etc/init.d

%files
%defattr(-, %{exogeni_user_id}, %{exogeni_group_id})
%attr(755, %{exogeni_user_id}, %{exogeni_group_id}) %dir %{homedir}
%attr(755, %{exogeni_user_id}, %{exogeni_group_id}) %dir %{log_dir}
%attr(755, %{exogeni_user_id}, %{exogeni_group_id}) %dir %{pid_dir}
%attr(750, %{exogeni_user_id}, root) %dir %{conf_dir}
%{homedir}/bin
%{homedir}/conf
%{homedir}/lib
%{homedir}/repo
%doc LICENSE README xoschema.sql

%changelog
*Tue Sep 09 2014 Jonathan Mills <jonmills@renci.org> - 0.2-16
- Fix for orca reservation id popping up in wrong place
*Wed Sep 03 2014 Jonathan Mills <jonmills@renci.org> - 0.2-15
- Updated to support db schema of ops-monitoring 2.0
*Wed Aug 27 2014 Victor J. Orlikowski <vjo@cs.duke.edu>
- Checked into subversion
