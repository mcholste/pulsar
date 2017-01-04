FROM debian:latest

RUN apt-get update -qq && apt-get install -y wget &&\
	wget -qO - http://download.opensuse.org/repositories/home:/laszlo_budai:/syslog-ng/Debian_8.0/Release.key | apt-key add - &&\
	echo 'deb http://download.opensuse.org/repositories/home:/laszlo_budai:/syslog-ng/Debian_8.0 ./' | tee --append /etc/apt/sources.list.d/syslog-ng-obs.list &&\
	apt-get update -qq && apt-get install -y \
	    syslog-ng \
	    python-pip \
	    python-dev &&\
	mkdir -p /usr/local/share/GeoIP &&\
	wget -qO - http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz | gunzip - > /usr/local/share/GeoIP/GeoLite2-City.mmdb &&\
	wget -qO - https://github.com/maxmind/libmaxminddb/releases/download/1.2.0/libmaxminddb-1.2.0.tar.gz > libmaxminddb-1.2.0.tar.gz &&\
	tar xzvf libmaxminddb-1.2.0.tar.gz && cd libmaxminddb-1.2.0 && ./configure && make && make install &&\
	pip install --upgrade ujson \
		elasticsearch \
		geoip2 &&\
	ldconfig

ADD conf/openjdk-libjvm.conf /etc/ld.so.conf.d/openjdk-libjvm.conf
ADD conf/syslog-ng-3.5.conf /etc/syslog-ng/syslog-ng.conf
ADD conf/pulsar.conf /etc/pulsar.conf
ADD . /opt/pulsar

WORKDIR /opt/pulsar

EXPOSE 514
EXPOSE 601
EXPOSE 6514

ENTRYPOINT ["/usr/sbin/syslog-ng", "-F"]
