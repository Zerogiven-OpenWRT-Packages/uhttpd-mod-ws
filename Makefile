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

define Build/Prepare
	mkdir -p $(PKG_BUILD_DIR)
	$(CP) ./src/* $(PKG_BUILD_DIR)/
	# uhttpd has no Build/InstallDev, so its plugin headers never land
	# in STAGING_DIR. PKG_BUILD_DEPENDS:=uhttpd guarantees its source is
	# already unpacked at $(BUILD_DIR)/uhttpd-<version>/ by now -- copy
	# the two headers we need alongside our source so the existing
	# #include "uhttpd.h" and "plugin.h" resolve as siblings.
	$(CP) $(BUILD_DIR)/uhttpd-*/uhttpd.h $(PKG_BUILD_DIR)/
	$(CP) $(BUILD_DIR)/uhttpd-*/plugin.h $(PKG_BUILD_DIR)/
endef

define Build/Compile
	$(MAKE) -C $(PKG_BUILD_DIR) \
		CC="$(TARGET_CC)" \
		CFLAGS="$(TARGET_CFLAGS) -I$(STAGING_DIR)/usr/include \
			-D_GNU_SOURCE -Wall -Wextra -Os" \
		LDFLAGS="$(TARGET_LDFLAGS) -L$(STAGING_DIR)/usr/lib"
endef

define Package/$(PKG_NAME)/install
	$(INSTALL_DIR) $(1)/usr/lib/uhttpd $(1)/www/luci-static/resources
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/uhttpd_ws.so $(1)/usr/lib/
	$(INSTALL_DATA) ./files/www/luci-static/resources/rpc-ws.js $(1)//www/luci-static/resources/rpc-ws.js
endef

$(eval $(call BuildPackage,uhttpd-mod-ws))
