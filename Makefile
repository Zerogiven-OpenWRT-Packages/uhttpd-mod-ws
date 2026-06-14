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

define Package/uhttpd-mod-ws
  SECTION:=net
  CATEGORY:=Network
  SUBMENU:=Web Servers/Proxies
  TITLE:=WebSocket (RFC 6455) transport for ubus JSON-RPC
  URL:=https://github.com/zerogiven/owrt-uhttpd-mod-ws
  DEPENDS:=uhttpd +libubus +libubox +libblobmsg-json +libjson-c +libwebsockets-openssl||libwebsockets-mbedtls
endef

define Package/uhttpd-mod-ws/description
	WebSocket transport plugin for uhttpd ubus JSON-RPC.
endef

# define Build/Prepare
# 	mkdir -p $(PKG_BUILD_DIR)
# 	$(CP) ./src/* $(PKG_BUILD_DIR)/
# endef

define Build/Compile
	$(MAKE) -C $(PKG_BUILD_DIR) \
		CC="$(TARGET_CC)" \
		CFLAGS="$(TARGET_CFLAGS) -I$(STAGING_DIR)/usr/include \
			-I$(STAGING_DIR)/usr/include/uhttpd \
			-D_GNU_SOURCE -Wall -Wextra -Os" \
		LDFLAGS="$(TARGET_LDFLAGS) -L$(STAGING_DIR)/usr/lib"
endef

define Package/uhttpd-mod-ws/install
	$(INSTALL_DIR) $(1)/usr/lib/uhttpd
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/uhttpd_ws.so \
		$(1)/usr/lib/uhttpd/uhttpd_ws.so

	# Bundled LuCI client (rpc-ws.js) and any other staged resources.
	# files/<path-on-device> -> $(1)/<path-on-device>.
	# Mirrors LuCI's resource convention so 'require rpc-ws' resolves
	# at /www/luci-static/resources/rpc-ws.js as soon as LuCI is present.
	# No hard luci-base dep -- the file is harmless without LuCI; useful
	# the moment LuCI is installed alongside.
	$(INSTALL_DIR) $(1)
	$(CP) ./files/. $(1)/
endef

$(eval $(call BuildPackage,uhttpd-mod-ws))
