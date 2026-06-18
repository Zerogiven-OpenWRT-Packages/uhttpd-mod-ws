#
# Copyright (C) 2026
# This is free software, licensed under the Apache License 2.0.
#

include $(TOPDIR)/rules.mk

PKG_NAME:=uhttpd-mod-ws
PKG_VERSION:=0.1.0
PKG_RELEASE:=1

PKG_LICENSE:=Apache-2.0
PKG_LICENSE_FILES:=LICENSE

PKG_BUILD_PARALLEL:=1

PKG_BUILD_DEPENDS:=uhttpd

include $(INCLUDE_DIR)/package.mk

define Package/$(PKG_NAME)
  SECTION:=net
  CATEGORY:=Network
  SUBMENU:=Web Servers/Proxies
  TITLE:=WebSocket transport for ubus JSON RPC
  URL:=https://github.com/zerogiven/owrt-uhttpd-mod-ws
  DEPENDS:=uhttpd +libubus +libubox +libblobmsg-json +libjson-c +libwebsockets
endef

define Package/$(PKG_NAME)/description
  WebSocket transport plugin for uhttpd ubus JSON RPC.
endef

define Build/Compile
	$(MAKE) -C $(PKG_BUILD_DIR) \
		CC="$(TARGET_CC)" \
		CFLAGS="$(TARGET_CFLAGS) -I$(STAGING_DIR)/usr/include \
			-fPIC -std=gnu99 -fvisibility=hidden \
			-DHAVE_TLS \
			-D_GNU_SOURCE -Wall -Wextra -Os" \
		LDFLAGS="$(TARGET_LDFLAGS) -L$(STAGING_DIR)/usr/lib"
endef

define Package/$(PKG_NAME)/install
	$(INSTALL_DIR) $(1)/usr/lib/uhttpd $(1)/www/luci-static/resources
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/uhttpd_ws.so $(1)/usr/lib/uhttpd/
	$(INSTALL_DATA) ./files/www/luci-static/resources/rpc-ws.js $(1)/www/luci-static/resources/rpc-ws.js
endef

# uhttpd dlopen()s plugins from /usr/lib/uhttpd at startup, so a freshly
# installed .so is invisible until uhttpd is restarted. Skip when staging
# into an offline image build (IPKG_INSTROOT set).
define Package/$(PKG_NAME)/postinst
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0
/etc/init.d/uhttpd enabled && /etc/init.d/uhttpd reload >/dev/null 2>&1
exit 0
endef

$(eval $(call BuildPackage,uhttpd-mod-ws))
