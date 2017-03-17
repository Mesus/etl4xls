# -*- coding: utf-8 -*-
sF = '长沙农场2016041521每日采收表'
iM = sF.find('每',0)-10
sFileDate = sF[0:iM]
print sFileDate.replace('农场','')

